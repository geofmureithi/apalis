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
    fmt::{self, Debug, Formatter},
    ops::Deref,
    panic::UnwindSafe,
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll},
};

use futures_util::{future::BoxFuture, Future, FutureExt, StreamExt};
use pin_project_lite::pin_project;
use tower_layer::Layer;
use tower_service::Service;

use crate::{
    backend::Backend,
    error::{BoxDynError, WorkerError},
    monitor::shutdown::Shutdown,
    task::Task,
    worker::{
        context::WorkerContext,
        event::{Event, EventHandler},
        ReadinessService, TrackerService, Worker,
    },
};

pub mod shutdown;

pin_project! {
    pub struct MonitoredWorker {
        factory: Box<dyn Fn(usize) -> (WorkerContext, BoxFuture<'static, Result<(), WorkerError>>)>,
        #[pin]
        current: Option<(WorkerContext, BoxFuture<'static, Result<(), WorkerError>>)>,
        attempt: usize,
        should_restart: Arc<RwLock<Option<Box<dyn Fn(&WorkerContext, &WorkerError, usize) -> bool + 'static + Send>>>>,
    }
}

impl Future for MonitoredWorker {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        use std::panic::{catch_unwind, AssertUnwindSafe};
        let mut this = self.project();

        loop {
            if this.current.is_none() {
                println!("Start {:?}", *this.attempt);
                let worker = (this.factory)(*this.attempt);
                this.current.set(Some(worker));
            }

            let mut current = this.current.as_mut().as_pin_mut().unwrap();
            let poll_result = catch_unwind(AssertUnwindSafe(|| current.1.as_mut().poll(cx)))
                .map_err(|err| {
                    let err = if let Some(s) = err.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = err.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "Unknown panic".to_string()
                    };
                    WorkerError::PanicError(err)
                });

            match poll_result {
                Ok(Poll::Pending) => return Poll::Pending,
                Ok(Poll::Ready(Ok(()))) => return Poll::Ready(()),
                Ok(Poll::Ready(Err(e))) | Err(e) => {
                    println!("Worker stopping, attempt: {}", *this.attempt);
                    let (ctx, _) = this.current.take().unwrap();
                    *this.attempt += 1;
                    let should_restart = this.should_restart.read();
                    match should_restart.as_ref().map(|s| s.as_ref()) {
                        Ok(Some(cb)) => {
                            if !(cb)(&ctx, &e, *this.attempt) {
                                return Poll::Ready(());
                            }
                        }
                        _ => return Poll::Ready(()),
                    }
                }
            }
        }
    }
}

/// A monitor for coordinating and managing a collection of workers.
pub struct Monitor {
    workers: Vec<MonitoredWorker>,
    terminator: Option<BoxFuture<'static, ()>>,
    shutdown: Shutdown,
    event_handler: EventHandler,
    should_restart: Arc<
        RwLock<Option<Box<dyn Fn(&WorkerContext, &WorkerError, usize) -> bool + 'static + Send>>>,
    >,
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
    fn run_worker<Args, S, P, Meta, M>(
        mut ctx: WorkerContext,
        worker: Worker<Args, Meta, P, S, M>,
    ) -> BoxFuture<'static, Result<(), WorkerError>>
    where
        S: Service<Task<Args, Meta, P::IdType>> + Send + 'static,
        S::Future: Send,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        P: Backend<Args, Meta> + Send + 'static,
        P::Error: Into<BoxDynError> + Send + 'static,
        P::Stream: Unpin + Send + 'static,
        P::Beat: Unpin + Send,
        Args: Send + 'static,
        Meta: Send + 'static,
        M: Layer<ReadinessService<TrackerService<S>>> + 'static,
        P::Layer: Layer<M::Service>,
        <P::Layer as Layer<M::Service>>::Service:
            Service<Task<Args, Meta, P::IdType>> + Send + 'static,
        <<P::Layer as Layer<M::Service>>::Service as Service<Task<Args, Meta, P::IdType>>>::Error:
            Into<BoxDynError> + Send + Sync + 'static,
        <<P::Layer as Layer<M::Service>>::Service as Service<Task<Args, Meta, P::IdType>>>::Future:
            Send,
        M::Service: Service<Task<Args, Meta, P::IdType>> + Send + 'static,
        <<M as Layer<ReadinessService<TrackerService<S>>>>::Service as Service<
            Task<Args, Meta, P::IdType>,
        >>::Future: Send,
        <<M as Layer<ReadinessService<TrackerService<S>>>>::Service as Service<
            Task<Args, Meta, P::IdType>,
        >>::Error: Into<BoxDynError> + Send + Sync + 'static,
        P::IdType: Sync + Send + 'static,
    {
        let mut stream = worker.stream_with_ctx(&mut ctx);
        async move {
            loop {
                match stream.next().await {
                    Some(Err(e)) => return Err(e),
                    None => return Ok(()),
                    _ => (),
                }
            }
        }
        .boxed()
    }
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
    pub fn register<Args, S, P, Meta, M>(
        mut self,
        factory: impl Fn(usize) -> Worker<Args, Meta, P, S, M> + 'static,
    ) -> Self
    where
        S: Service<Task<Args, Meta, P::IdType>> + Send + 'static,
        S::Future: Send,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        P: Backend<Args, Meta> + Send + 'static,
        P::Error: Into<BoxDynError> + Send + 'static,
        P::Stream: Unpin + Send + 'static,
        P::Beat: Unpin + Send,
        Args: Send + 'static,
        Meta: Send + 'static,
        M: Layer<ReadinessService<TrackerService<S>>> + 'static,
        P::Layer: Layer<M::Service>,
        <P::Layer as Layer<M::Service>>::Service:
            Service<Task<Args, Meta, P::IdType>> + Send + 'static,
        <<P::Layer as Layer<M::Service>>::Service as Service<Task<Args, Meta, P::IdType>>>::Error:
            Into<BoxDynError> + Send + Sync + 'static,
        <<P::Layer as Layer<M::Service>>::Service as Service<Task<Args, Meta, P::IdType>>>::Future:
            Send,
        M::Service: Service<Task<Args, Meta, P::IdType>> + Send + 'static,
        <<M as Layer<ReadinessService<TrackerService<S>>>>::Service as Service<
            Task<Args, Meta, P::IdType>,
        >>::Future: Send,
        <<M as Layer<ReadinessService<TrackerService<S>>>>::Service as Service<
            Task<Args, Meta, P::IdType>,
        >>::Error: Into<BoxDynError> + Send + Sync + 'static,
        P::IdType: Send + Sync + 'static,
    {
        let shutdown = Some(self.shutdown.clone());
        let handler = self.event_handler.clone();
        let should_restart = self.should_restart.clone();
        // let h = handler.clone();
        // ctx.wrap_listener(move |ctx, event| {
        //     match h.read() {
        //         Ok(inner) => {
        //             if let Some(h) = inner.as_deref() {
        //                 h(&ctx, &event);
        //             }
        //         }
        //         Err(_) => todo!(),
        //     };
        // });

        let worker = MonitoredWorker {
            current: None,
            factory: Box::new(move |attempt| {
                let new_worker = factory(attempt);
                let id = Arc::new(new_worker.name.clone());
                let mut ctx = WorkerContext::new::<M::Service>(&id);
                ctx.shutdown = shutdown.clone();
                (ctx.clone(), Self::run_worker(ctx.clone(), new_worker))
            }),
            attempt: 0,
            should_restart,
        };
        self.workers.push(worker);
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
            let _res = futures_util::future::select(
                futures_util::future::join_all(self.workers).map(|_| shutdown.start_shutdown()),
                async {
                    let _res = shutdown_after.await;
                    terminator.await;
                }
                .boxed(),
            )
            .await;
        } else {
            let runner = self.run();
            let res = futures_util::join!(shutdown_after, runner); // If no terminator is provided, we wait for both the shutdown call and all workers to complete
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
        futures_util::join!(
            futures_util::future::join_all(self.workers).map(|_| shutdown.start_shutdown()),
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
            workers: Vec::new(),
            should_restart: Default::default(), // Don't restart
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

    /// Allows controlling the restart strategy for workers
    pub fn should_restart<F: Fn(&WorkerContext, &WorkerError, usize) -> bool + Send + 'static>(
        self,
        cb: F,
    ) -> Self {
        let _ = self.should_restart.write().map(|mut res| {
            let _ = res.insert(Box::new(cb));
        });
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        backend::{Backend, BackendWithSink, TaskSink},
        task::task_id::TaskId,
        worker::{context::WorkerContext, ext::event_listener::EventListenerExt},
    };
    use core::panic;
    use std::{io, time::Duration};

    use tokio::time::sleep;
    use tower::limit::ConcurrencyLimitLayer;

    use crate::{
        backend::memory::MemoryStorage, monitor::Monitor, task::Task,
        worker::builder::WorkerBuilder,
    };

    #[tokio::test]
    async fn it_works_with_workers() {
        let mut backend = MemoryStorage::new_with_json();

        for i in 0..10 {
            backend.sink().push(i).await.unwrap();
        }

        let service = tower::service_fn(|request: Task<u32, ()>| async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok::<_, io::Error>(request)
        });
        let monitor: Monitor = Monitor::new();
        let monitor = monitor.register(move |index| {
            WorkerBuilder::new(format!("rango-tango-{index}"))
                .backend(backend.clone())
                .build(service.clone())
        });
        let shutdown = monitor.shutdown.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(1500)).await;
            shutdown.start_shutdown();
        });
        monitor.run().await.unwrap();
    }
    #[tokio::test]
    async fn test_monitor_run() {
        let mut backend = MemoryStorage::new_with_json();
        let mut sink = backend.sink();

        tokio::spawn(async move {
            for i in 0..10 {
                sink.push(i).await.unwrap();
                tokio::time::sleep(Duration::from_millis(i)).await;
            }
        });

        let monitor: Monitor = Monitor::new()
            .register(move |index| {
                WorkerBuilder::new(format!("rango-tango-{index}"))
                    .backend(backend.clone())
                    .on_event(|wrk, e| {
                        println!("{}, {e:?}", wrk.name());
                        panic!("Brrr");
                    })
                    .build(move |r| async move {
                        if r % 5 == 0 {
                            panic!("Brrr")
                        }
                    })
            })
            .should_restart(|ctx, e, index| {
                println!("{ctx:?}, {e:?}, {index}");
                if index > 10 {
                    return false;
                }
                return true;
            })
            .on_event(|wrk, e| {
                println!("{}, {e:?}", wrk.name());
            });
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
        let mut backend = MemoryStorage::new_with_json();
        let mut sink = backend.sink();
        for i in 0..10 {
            sink.push(i).await.unwrap();
        }

        let monitor: Monitor = Monitor::new();

        let monitor = monitor.on_event(|wrk, e| {
            println!("{:?}, {e:?}", wrk.name());
        });

        let monitor = monitor.register(move |index| {
            WorkerBuilder::new(format!("worker-{}", index))
                .backend(backend.clone())
                .layer(ConcurrencyLimitLayer::new(1))
                .build(
                    move |request: i32, id: TaskId, w: WorkerContext| async move {
                        println!("{id:?}, {}", w.name());
                        tokio::time::sleep(Duration::from_secs(index as u64)).await;
                        Ok::<_, io::Error>(request)
                    },
                )
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
