//! Represents utilities for monitoring of running workers
//!
//! The `Monitor` provides centralized coordination and lifecycle management of multiple [`Worker`] instances.
//! It is responsible for executing, monitoring, and gracefully shutting down all registered workers in a robust and customizable manner.
//!
//! ## Features
//!
//! - Register and run one or more workers.
//! - Handle graceful shutdown with optional timeout.
//! - Register custom event handlers to observe worker events (e.g. job received, completed, errored).
//! - Integrate shutdown with system signals (e.g. `SIGINT`, `SIGTERM`) or custom triggers.
//!
//! ## Usage
//!
//! ```rust
//! use apalis_core::monitor::Monitor;
//! use apalis_core::worker::WorkerBuilder;
//! use apalis_core::memory::MemoryStorage;
//! use apalis_core::request::Request;
//! use tower::service_fn;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut storage = MemoryStorage::new();
//!     let sink = storage.sink();
//!     // Push some jobs into the backend
//!     for i in 0..5 {
//!         sink.push(i).await.unwrap();
//!     }
//!
//!     let service = service_fn(|req: Request<u32, ()>| async move {
//!         println!("Processing job: {:?}", req));
//!         Ok::<_, std::io::Error>(req)
//!     });
//!
//!     let worker = WorkerBuilder::new("demo-worker")
//!         .backend(storage)
//!         .build(service);
//!
//!     let monitor = Monitor::new()
//!         .on_event(|ctx, event| println!("{}: {:?}", ctx.id(), event))
//!         .register(worker);
//!
//!     // Start monitor and run all registered workers
//!     monitor.run().await.unwrap();
//! }
//! ```
//!
//! ## Graceful Shutdown with Timeout
//!
//! If you want the monitor to force shutdown after a certain duration, use the `shutdown_timeout` method:
//!
//! ```rust
//! # use apalis_core::monitor::Monitor;
//! # use std::time::Duration;
//! let monitor = Monitor::new()
//!     .shutdown_timeout(Duration::from_secs(10));
//! ```
//!
//! This ensures that if any worker hangs or takes too long to finish, the monitor will shut down after 10 seconds.

use std::{
    any::type_name,
    collections::HashMap,
    fmt::{self, Debug, Formatter},
    future::ready,
    sync::Arc,
};

use futures::{
    future::{select, BoxFuture},
    Future, FutureExt, StreamExt,
};
use tower::{Layer, Service};

use crate::{
    backend::Backend,
    error::BoxDynError,
    monitor::shutdown::Shutdown,
    request::Request,
    worker::{
        context::WorkerContext,
        event::{Event, EventHandler},
        ReadinessService, TrackerService, Worker,
    },
};

pub mod shutdown;

struct MonitoredWorker {
    ctx: WorkerContext,
    fut: BoxFuture<'static, ()>,
}

/// A monitor for coordinating and managing a collection of workers.
pub struct Monitor {
    workers: HashMap<Arc<String>, MonitoredWorker>,
    terminator: Option<BoxFuture<'static, ()>>,
    shutdown: Shutdown,
    event_handler: EventHandler,
}

impl Debug for Monitor {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Monitor")
            .field("shutdown", &"[Graceful shutdown listener]")
            .field("workers", &self.workers.len())
            .finish()
    }
}

impl Monitor {
    /// Registers a single instance of a [Worker]
    ///
    /// # Examples
    ///
    /// ```
    /// use apalis_core::monitor::Monitor;
    ///
    /// let monitor = Monitor::new();
    /// let worker = Worker::new();
    /// monitor.register(worker).run().await;
    /// ```
    pub fn register<Args, S, P, Ctx, M>(mut self, mut worker: Worker<Args, Ctx, P, S, M>) -> Self
    where
        S: Service<Request<Args, Ctx>> + Send + 'static,
        S::Future: Send,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        P: Backend<Args, Ctx> + Send + 'static,
        P::Error: Into<BoxDynError> + Send + 'static,
        P::Stream: Unpin + Send + 'static,
        P::Beat: Unpin + Send,
        Args: Send + 'static,
        Ctx: Send + 'static,
        M: Layer<ReadinessService<TrackerService<S>>> + 'static,
        P::Layer: Layer<M::Service>,
        <P::Layer as Layer<M::Service>>::Service: Service<Request<Args, Ctx>> + Send + 'static,
        <<P::Layer as Layer<M::Service>>::Service as Service<Request<Args, Ctx>>>::Error:
            Into<BoxDynError> + Send + Sync + 'static,
        <<P::Layer as Layer<M::Service>>::Service as Service<Request<Args, Ctx>>>::Future: Send,
        M::Service: Service<Request<Args, Ctx>> + Send + 'static,
        <<M as Layer<ReadinessService<TrackerService<S>>>>::Service as Service<
            Request<Args, Ctx>,
        >>::Future: Send,
        <<M as Layer<ReadinessService<TrackerService<S>>>>::Service as Service<
            Request<Args, Ctx>,
        >>::Error: Into<BoxDynError> + Send + Sync + 'static,
    {
        let id = Arc::new(worker.name.clone());
        let mut ctx = WorkerContext::new::<M::Service>(&id);
        ctx.shutdown = Some(self.shutdown.clone());

        let handler = self.event_handler.clone();

        let stream = worker.stream_with_ctx(&mut ctx);
        let worker_ctx = ctx.clone();
        let stream = stream.for_each(move |e| match e {
            Ok(e) => {
                let ctx = worker_ctx.clone();
                let handler = handler.clone();
                async move {
                    match handler.read() {
                        Ok(inner) => {
                            if let Some(h) = inner.as_deref() {
                                h(&ctx, &e);
                            }
                        }
                        Err(_) => todo!(),
                    };
                }
                .boxed()
            }
            Err(e) => {
                let ctx = worker_ctx.clone();
                let handler = handler.clone();
                ctx.stop().unwrap();
                async move {
                    match handler.read() {
                        Ok(inner) => {
                            if let Some(h) = inner.as_deref() {
                                h(&ctx, &Event::Error(Arc::new(Box::new(e))));
                            }
                        }
                        Err(_) => todo!(),
                    };
                }
                .boxed()
            }
        });
        let worker = MonitoredWorker {
            ctx,
            fut: stream.boxed(),
        };
        self.workers.insert(id, worker);
        self
    }

    /// Registers multiple workers with the monitor.
    ///
    /// # Arguments
    ///
    /// * `count` - The number of workers to register.
    /// * `worker` - A Worker that is ready for running.
    ///
    /// # Returns
    ///
    /// The monitor instance, with all workers added to the collection.
    pub fn register_with_count<Args, S, P, Ctx, M>(
        mut self,
        count: usize,
        worker_fn: impl Fn(usize) -> Worker<Args, Ctx, P, S, M>,
    ) -> Self
    where
        S: Service<Request<Args, Ctx>> + Send + 'static,
        S::Future: Send,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        P: Backend<Args, Ctx> + Send + 'static,
        P::Error: Into<BoxDynError> + Send + 'static,
        P::Stream: Unpin + Send + 'static,
        P::Beat: Unpin + Send,
        Args: Send + 'static,
        Ctx: Send + 'static,
        M: Layer<ReadinessService<TrackerService<S>>> + 'static,
        P::Layer: Layer<M::Service>,
        <P::Layer as Layer<M::Service>>::Service: Service<Request<Args, Ctx>> + Send + 'static,
        <<P::Layer as Layer<M::Service>>::Service as Service<Request<Args, Ctx>>>::Error:
            Into<BoxDynError> + Send + Sync + 'static,
        <<P::Layer as Layer<M::Service>>::Service as Service<Request<Args, Ctx>>>::Future: Send,
        M::Service: Service<Request<Args, Ctx>> + Send + 'static,
        <<M as Layer<ReadinessService<TrackerService<S>>>>::Service as Service<
            Request<Args, Ctx>,
        >>::Future: Send,
        <<M as Layer<ReadinessService<TrackerService<S>>>>::Service as Service<
            Request<Args, Ctx>,
        >>::Error: Into<BoxDynError> + Send + Sync + 'static,
    {
        for index in 0..count {
            let worker = worker_fn(index);
            self = self.register(worker);
        }
        self
    }
    /// Runs the monitor and all its registered workers until they have all completed or a shutdown signal is received.
    ///
    /// # Arguments
    ///
    /// * `signal` - A `Future` that resolves when a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// If the monitor fails to shutdown gracefully, an `std::io::Error` will be returned.
    ///
    /// # Remarks
    ///
    /// If a timeout has been set using the `Monitor::shutdown_timeout` method, the monitor
    /// will wait for all workers to complete up to the timeout duration before exiting.
    /// If the timeout is reached and workers have not completed, the monitor will exit forcefully.
    pub async fn run_with_signal<S>(self, signal: S) -> std::io::Result<()>
    where
        S: Send + Future<Output = std::io::Result<()>>,
    {
        let shutdown = self.shutdown.clone();
        let shutdown_after = self.shutdown.shutdown_after(signal);
        if let Some(terminator) = self.terminator {
            let _res = futures::future::select(
                futures::future::join_all(
                    self.workers
                        .into_iter()
                        .map(|(_, worker)| select(worker.ctx, worker.fut)),
                )
                .map(|_| shutdown.start_shutdown())
                .boxed(),
                async {
                    let _res = shutdown_after.await;
                    terminator.await;
                }
                .boxed(),
            )
            .await;
        } else {
            let runner = self.run();
            let res = futures::join!(shutdown_after, runner); // If no terminator is provided, we wait for both the shutdown call and all workers to complete
            match res {
                (Ok(_), Ok(_)) => {
                    // All good
                }
                (Err(e), Ok(_)) => return Err(e),
                (Ok(_), Err(e)) => return Err(e),
                (Err(e), Err(_)) => return Err(e),
            }
        }
        Ok(())
    }

    /// Runs the monitor and all its registered workers until they have all completed.
    ///
    /// # Errors
    ///
    /// If the monitor fails to run gracefully, an `std::io::Error` will be returned.
    ///
    /// # Remarks
    ///
    /// If all workers have completed execution, then by default the monitor will start a shutdown
    pub async fn run(self) -> std::io::Result<()> {
        let shutdown = self.shutdown.clone();
        let shutdown_future = self.shutdown.boxed().map(|_| ());
        futures::join!(
            futures::future::join_all(self.workers.into_iter().map(|(_, mut worker)| {
                worker.ctx.start().expect("Worker should be able to start");
                select(worker.ctx, worker.fut)
            }))
            .map(|_| shutdown.start_shutdown()),
            shutdown_future,
        );

        Ok(())
    }

    /// Handles all workers' events emitted
    pub fn on_event<F: Fn(&WorkerContext, &Event) + Send + Sync + 'static>(self, f: F) -> Self {
        let _ = self.event_handler.write().map(|mut res| {
            let _ = res.insert(Box::new(f));
        });
        self
    }
}

impl Default for Monitor {
    fn default() -> Self {
        Self {
            shutdown: Shutdown::new(),
            terminator: None,
            event_handler: Arc::default(),
            workers: HashMap::new(),
        }
    }
}

impl Monitor {
    /// Creates a new monitor instance.
    ///
    /// # Returns
    ///
    /// A new monitor instance, with an empty collection of workers.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a timeout duration for the monitor's shutdown process.
    ///
    /// # Arguments
    ///
    /// * `duration` - The timeout duration.
    ///
    /// # Returns
    ///
    /// The monitor instance, with the shutdown timeout duration set.
    #[cfg(feature = "sleep")]
    pub fn shutdown_timeout(self, duration: std::time::Duration) -> Self {
        self.with_terminator(crate::timer::sleep(duration))
    }

    /// Sets a future that will start being polled when the monitor's shutdown process starts.
    ///
    /// After shutdown has been initiated, the `terminator` future will be run, and if it completes
    /// before all tasks are completed the shutdown process will complete, thus finishing the
    /// shutdown even if there are outstanding tasks. This can be useful for using a timeout or
    /// signal (or combination) to force a full shutdown even if one or more tasks are taking
    /// longer than expected to finish.
    pub fn with_terminator(mut self, fut: impl Future<Output = ()> + Send + 'static) -> Self {
        self.terminator = Some(fut.boxed());
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        backend::{Backend, TaskSink}, request::task_id::TaskId, worker::{context::WorkerContext, ext::event_listener::EventListenerExt}
    };
    use std::{io, time::Duration};

    use tokio::time::sleep;

    use crate::{
        backend::memory::MemoryStorage, monitor::Monitor, request::Request,
        worker::builder::WorkerBuilder,
    };

    #[tokio::test]
    async fn it_works_with_workers() {
        let mut backend = MemoryStorage::new();

        for i in 0..10 {
            backend.sink().push(i).await.unwrap();
        }

        let service = tower::service_fn(|request: Request<u32, ()>| async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok::<_, io::Error>(request)
        });
        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .build(service);
        let monitor: Monitor = Monitor::new();
        let monitor = monitor.register(worker);
        let shutdown = monitor.shutdown.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(1500)).await;
            shutdown.start_shutdown();
        });
        monitor.run().await.unwrap();
    }
    #[tokio::test]
    async fn test_monitor_run() {
        let mut backend = MemoryStorage::new();
        let mut sink = backend.sink();
        for i in 0..10 {
            sink.push(i).await.unwrap();
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|wrk, e| {
                println!("{}, {e:?}", wrk.name());
            })
            .build(|request| async move {
                tokio::time::sleep(Duration::from_millis(1000)).await;
                Ok::<_, io::Error>(request)
            });
        let monitor: Monitor = Monitor::new();

        let monitor = monitor.on_event(|wrk, e| {
            println!("{}, {e:?}", wrk.name());
        });

        let monitor = monitor.register(worker);
        assert_eq!(monitor.workers.len(), 1);
        let shutdown = monitor.shutdown.clone();

        tokio::spawn(async move {
            sleep(Duration::from_millis(5000)).await;
            shutdown.start_shutdown();
        });

        let result = monitor.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_monitor_register_with_count() {
        let backend = MemoryStorage::new_with_json();
        let mut sink = backend.sink();
        for i in 0..10 {
            sink.push(i).await.unwrap();
        }

        let monitor: Monitor = Monitor::new();

        let monitor = monitor.on_event(|wrk, e| {
            println!("{:?}, {e:?}", wrk.name());
        });

        let monitor = monitor.register_with_count(5, move |index| {
            WorkerBuilder::new(format!("worker-{}", index))
                .backend(backend.clone())
                .chain(|s| s.concurrency_limit(1))
                .build(move |request, id: TaskId, w: WorkerContext| async move {
                    println!("{id:?}, {}", w.name());
                    tokio::time::sleep(Duration::from_secs(index as u64)).await;
                    Ok::<_, io::Error>(request)
                })
        });
        assert_eq!(monitor.workers.len(), 5);
        let shutdown = monitor.shutdown.clone();

        tokio::spawn(async move {
            sleep(Duration::from_millis(5000)).await;
            shutdown.start_shutdown();
        });

        let result = monitor.run().await;
        assert!(result.is_ok());
    }
}
