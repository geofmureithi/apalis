//! # Monitor
//!
//! `Monitor` provides centralized coordination and lifecycle management for multiple workers.
//! It is responsible for executing, monitoring, and gracefully shutting down all registered workers in a robust and customizable manner.
//!
//! ## Features
//!
//! - Register and run one or more workers.
//! - Handle graceful shutdown with optional timeout.
//! - Register custom event handlers to observe worker events (e.g. task received, completed, errored).
//! - Integrate shutdown with system signals (e.g. `SIGINT`, `SIGTERM`) or custom triggers.
//! - **Restart Strategy:** Configure custom logic to automatically restart workers on failure.
//!
//! ## Usage
//!
//! ```rust
//! use apalis_core::monitor::Monitor;
//! use apalis_core::worker::WorkerBuilder;
//! use apalis_core::memory::MemoryStorage;
//! use apalis_core::task::Task;
//! use tower::service_fn;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut storage = MemoryStorage::new();
//!     for i in 0..5 {
//!         storage.push(i).await.unwrap();
//!     }
//!
//!     let service = service_fn(|req: Task<u32>| async move {
//!         println!("Processing task: {:?}", req);
//!         Ok::<_, std::io::Error>(req)
//!     });
//!
//!     let worker = WorkerBuilder::new("demo-worker")
//!         .backend(storage)
//!         .build(service);
//!
//!     let monitor = Monitor::new()
//!         .on_event(|ctx, event| println!("{}: {:?}", ctx.id(), event))
//!         .register(|_| worker);
//!
//!     // Start monitor and run all registered workers
//!     monitor.run().await.unwrap();
//! }
//! ```
//!
//! ## Graceful Shutdown with Timeout
//!
//! To force shutdown after a certain duration, use the `shutdown_timeout` method:
//!
//! ```rust
//! # use apalis_core::monitor::Monitor;
//! # use std::time::Duration;
//! let monitor = Monitor::new()
//!     .shutdown_timeout(Duration::from_secs(10))
//!     .run_with_signal(signal::ctrl_c())
//!     .await
//!     .unwrap();
//! ```
//!
//! This ensures that if any worker hangs or takes too long to finish, the monitor will shut down after 10 seconds.
//!
//! ## Restarting Workers on Failure
//!
//! You can configure the monitor to restart workers when they fail, using custom logic:
//!
//! ```rust
//! use apalis_core::monitor::Monitor;
//!
//! let monitor = Monitor::new()
//!     .should_restart(|_ctx, error, attempt| {
//!         println!("Worker failed: {error:?}, attempt: {attempt}");
//!         attempt < 3 // Restart up to 3 times
//!     });
//! ```
//!
//! ## Observing Worker Events
//!
//! Register event handlers to observe worker lifecycle events:
//!
//! ```rust
//! let monitor = Monitor::new()
//!     .on_event(|ctx, event| println!("Worker {}: {:?}", ctx.name(), event));
//! ```
//!
//! ## Registering Multiple Workers
//!
//! You can register multiple workers using the `register` method. Each worker can be customized by index:
//!
//! ```rust
//! let monitor = Monitor::new()
//!     .register(|index| WorkerBuilder::new(format!("worker-{index}"))
//!         .backend(storage.clone())
//!         .build(|task| async move { /* ... */ }));
//! ```
//!
//! ## Example: Full Monitor Usage
//!
//! ```rust
//! let monitor = Monitor::new()
//!     .register(|index| WorkerBuilder::new(format!("worker-{index}"))
//!         .backend(storage.clone())
//!         .build(|task| async move { /* ... */ }))
//!     .should_restart(|_, _, attempt| attempt < 5)
//!     .on_event(|ctx, event| println!("Event: {:?}", event))
//!     .shutdown_timeout(Duration::from_secs(10));
//!
//! monitor.run().await.unwrap();
//! ```
//!
//! ## Error Handling
//!
//! If any worker fails, the monitor will return a `MonitorError` containing details about the failure.
//! You can inspect the error to see which workers failed and why.
//!
//! [Worker]: crate::worker::Worker

use std::{
    fmt::{self, Debug, Formatter},
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll},
};

use futures_util::{future::BoxFuture, Future, FutureExt, StreamExt};
use tower_layer::Layer;
use tower_service::Service;

use crate::{
    backend::Backend,
    error::{BoxDynError, WorkerError},
    monitor::shutdown::Shutdown,
    task::Task,
    worker::{
        context::WorkerContext,
        event::{Event, EventHandlerBuilder},
        ReadinessService, TrackerService, Worker,
    },
};

pub mod shutdown;

#[pin_project::pin_project]
/// A worker that is monitored by the [`Monitor`].
struct MonitoredWorker {
    factory: Box<
        dyn Fn(usize) -> (WorkerContext, BoxFuture<'static, Result<(), WorkerError>>)
            + Sync
            + 'static
            + Send,
    >,
    #[pin]
    current: Option<(WorkerContext, BoxFuture<'static, Result<(), WorkerError>>)>,
    attempt: usize,
    should_restart: Arc<
        RwLock<
            Option<
                Box<dyn Fn(&WorkerContext, &WorkerError, usize) -> bool + Sync + 'static + Send>,
            >,
        >,
    >,
}

/// Represents errors that occurred in a monitored worker, including its context and the error itself.
#[derive(Debug)]
pub struct MonitoredWorkerError {
    ctx: WorkerContext,
    error: WorkerError,
}

impl Future for MonitoredWorker {
    type Output = Result<(), MonitoredWorkerError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        use std::panic::{catch_unwind, AssertUnwindSafe};
        let mut this = self.project();

        loop {
            if this.current.is_none() {
                let worker = (this.factory)(*this.attempt);
                this.current.set(Some(worker));
            }

            let mut current = this.current.as_mut().as_pin_mut().unwrap();
            if current.0.is_running() && current.0.is_shutting_down() {
                return Poll::Ready(Ok(()));
            }
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
                Ok(Poll::Ready(Ok(()))) => return Poll::Ready(Ok(())),
                Ok(Poll::Ready(Err(e))) | Err(e) => {
                    let (ctx, _) = this.current.take().unwrap();
                    ctx.stop().unwrap();
                    let should_restart = this.should_restart.read();
                    match should_restart.as_ref().map(|s| s.as_ref()) {
                        Ok(Some(cb)) => {
                            if !(cb)(&ctx, &e, *this.attempt) {
                                return Poll::Ready(Err(MonitoredWorkerError { ctx, error: e }));
                            }
                            *this.attempt += 1;
                        }
                        _ => return Poll::Ready(Err(MonitoredWorkerError { ctx, error: e })),
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
    event_handler: EventHandlerBuilder,
    should_restart: Arc<
        RwLock<
            Option<
                Box<dyn Fn(&WorkerContext, &WorkerError, usize) -> bool + 'static + Send + Sync>,
            >,
        >,
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
    fn run_worker<Args, S, P, M>(
        mut ctx: WorkerContext,
        worker: Worker<Args, P::Ctx, P, S, M>,
    ) -> BoxFuture<'static, Result<(), WorkerError>>
    where
        S: Service<Task<Args, P::Ctx, P::IdType>> + Send + 'static,
        S::Future: Send,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        P: Backend<Args> + Send + 'static,
        P::Error: Into<BoxDynError> + Send + 'static,
        P::Stream: Unpin + Send + 'static,
        P::Beat: Unpin + Send,
        Args: Send + 'static,
        P::Ctx: Send + 'static,
        M: Layer<ReadinessService<TrackerService<S>>> + 'static,
        P::Layer: Layer<M::Service>,
        <P::Layer as Layer<M::Service>>::Service:
            Service<Task<Args, P::Ctx, P::IdType>> + Send + 'static,
        <<P::Layer as Layer<M::Service>>::Service as Service<Task<Args, P::Ctx, P::IdType>>>::Error:
            Into<BoxDynError> + Send + Sync + 'static,
        <<P::Layer as Layer<M::Service>>::Service as Service<Task<Args, P::Ctx, P::IdType>>>::Future:
            Send,
        M::Service: Service<Task<Args, P::Ctx, P::IdType>> + Send + 'static,
        <<M as Layer<ReadinessService<TrackerService<S>>>>::Service as Service<
            Task<Args, P::Ctx, P::IdType>,
        >>::Future: Send,
        <<M as Layer<ReadinessService<TrackerService<S>>>>::Service as Service<
            Task<Args, P::Ctx, P::IdType>,
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
    pub fn register<Args, S, P, M>(
        mut self,
        factory: impl Fn(usize) -> Worker<Args, P::Ctx, P, S, M> + 'static + Send + Sync,
    ) -> Self
    where
        S: Service<Task<Args, P::Ctx, P::IdType>> + Send + 'static,
        S::Future: Send,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        P: Backend<Args> + Send + 'static,
        P::Error: Into<BoxDynError> + Send + 'static,
        P::Stream: Unpin + Send + 'static,
        P::Beat: Unpin + Send,
        Args: Send + 'static,
        P::Ctx: Send + 'static,
        M: Layer<ReadinessService<TrackerService<S>>> + 'static,
        P::Layer: Layer<M::Service>,
        <P::Layer as Layer<M::Service>>::Service:
            Service<Task<Args, P::Ctx, P::IdType>> + Send + 'static,
        <<P::Layer as Layer<M::Service>>::Service as Service<Task<Args, P::Ctx, P::IdType>>>::Error:
            Into<BoxDynError> + Send + Sync + 'static,
        <<P::Layer as Layer<M::Service>>::Service as Service<Task<Args, P::Ctx, P::IdType>>>::Future:
            Send,
        M::Service: Service<Task<Args, P::Ctx, P::IdType>> + Send + 'static,
        <<M as Layer<ReadinessService<TrackerService<S>>>>::Service as Service<
            Task<Args, P::Ctx, P::IdType>,
        >>::Future: Send,
        <<M as Layer<ReadinessService<TrackerService<S>>>>::Service as Service<
            Task<Args, P::Ctx, P::IdType>,
        >>::Error: Into<BoxDynError> + Send + Sync + 'static,
        P::IdType: Send + Sync + 'static,
    {
        let shutdown = Some(self.shutdown.clone());
        let handler = self.event_handler.clone();
        let should_restart = self.should_restart.clone();
        let worker = MonitoredWorker {
            current: None,
            factory: Box::new(move |attempt| {
                let new_worker = factory(attempt);
                let id = Arc::new(new_worker.name.clone());
                let mut ctx = WorkerContext::new::<M::Service>(&id);
                let handler = handler.clone();
                ctx.wrap_listener(move |ctx, ev| {
                    let handlers = handler.read();
                    if let Ok(handlers) = handlers {
                        for h in handlers.iter() {
                            h(&ctx, &ev);
                        }
                    }
                });
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
    pub async fn run_with_signal<S>(self, signal: S) -> Result<(), MonitorError>
    where
        S: Send + Future<Output = std::io::Result<()>>,
    {
        let shutdown = self.shutdown.clone();
        let shutdown_after = self.shutdown.shutdown_after(signal);
        if let Some(terminator) = self.terminator {
            let _res = futures_util::future::select(
                Self::run_all_workers(self.workers, shutdown).boxed(),
                async {
                    let res = shutdown_after.await;
                    terminator.await;
                    res.map_err(|e| MonitorError::ShutdownSignal(e))
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
                (Err(e), Ok(_)) => return Err(e.into()),
                (Ok(_), Err(e)) => return Err(e),
                (Err(e), Err(_)) => return Err(e.into()),
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
    pub async fn run(self) -> Result<(), MonitorError> {
        let shutdown = self.shutdown.clone();
        let shutdown_future = self.shutdown.boxed().map(|_| ());
        let (result, _) = futures_util::join!(
            Self::run_all_workers(self.workers, shutdown),
            shutdown_future,
        );

        Ok(result?)
    }
    async fn run_all_workers(
        workers: Vec<MonitoredWorker>,
        shutdown: Shutdown,
    ) -> Result<(), MonitorError> {
        let results = futures_util::future::join_all(workers).await;

        shutdown.start_shutdown();

        let mut errors = Vec::new();
        // Check if any worker errored
        for r in results {
            if let Err(e) = r {
                errors.push(e);
            }
        }
        if !errors.is_empty() {
            return Err(MonitorError::ExitError(ExitError(errors)));
        }
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
    pub fn should_restart<F>(self, cb: F) -> Self
    where
        F: Fn(&WorkerContext, &WorkerError, usize) -> bool + Send + Sync + 'static,
    {
        let _ = self.should_restart.write().map(|mut res| {
            let _ = res.insert(Box::new(cb));
        });
        self
    }
}

/// Error type for monitor operations.
#[derive(Debug, thiserror::Error)]
pub enum MonitorError {
    /// Error occurred while running one or more workers
    #[error("Worker errors:\n{0}")]
    ExitError(#[from] ExitError),

    /// Error occurred while waiting for shutdown signal
    #[error("Shutdown signal error: {0}")]
    ShutdownSignal(#[from] std::io::Error),
}

impl fmt::Debug for ExitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        std::fmt::Display::fmt(&self, f)
    }
}

/// Represents errors that occurred in a monitored worker, including its context and the error itself.
#[derive(thiserror::Error)]
pub struct ExitError(pub Vec<MonitoredWorkerError>);

impl std::fmt::Display for ExitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "MonitoredErrors:")?;
        for worker in &self.0 {
            writeln!(f, " - Worker `{}`: {}", worker.ctx.name(), worker.error)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backend::{json::JsonStorage, TaskSink},
        task::task_id::TaskId,
        worker::context::WorkerContext,
    };
    use core::panic;
    use std::{io, time::Duration};

    use tokio::time::sleep;
    use tower::limit::ConcurrencyLimitLayer;

    use crate::{monitor::Monitor, worker::builder::WorkerBuilder};

    #[tokio::test]
    async fn basic_with_workers() {
        let mut backend = JsonStorage::new_temp().unwrap();

        for i in 0..10 {
            backend.push(i).await.unwrap();
        }

        let monitor: Monitor = Monitor::new();
        let monitor = monitor.register(move |index| {
            WorkerBuilder::new(format!("rango-tango-{index}"))
                .backend(backend.clone())
                .build(move |r: u32, id: TaskId, w: WorkerContext| async move {
                    println!("{id:?}, {}", w.name());
                    tokio::time::sleep(Duration::from_secs(index as u64)).await;
                    Ok::<_, io::Error>(r)
                })
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
        let mut backend = JsonStorage::new(
            "/var/folders/h_/sd1_gb5x73bbcxz38dts7pj80000gp/T/apalis-json-store-girmm9e36pz",
        )
        .unwrap();

        for i in 0..10 {
            backend.push(i).await.unwrap();
        }

        let monitor: Monitor = Monitor::new()
            .register(move |index| {
                WorkerBuilder::new(format!("rango-tango-{index}"))
                    .backend(backend.clone())
                    .build(move |r| async move {
                        if r % 2 == 0 {
                            panic!("Brrr")
                        }
                    })
            })
            .should_restart(|ctx, e, index| {
                println!(
                    "Encountered error in {} with {e:?} for attempt {index}",
                    ctx.name()
                );
                if index > 3 {
                    return false;
                }
                return true;
            })
            .on_event(|wrk, e| {
                println!("{}: {e:?}", wrk.name());
            });
        assert_eq!(monitor.workers.len(), 1);
        let shutdown = monitor.shutdown.clone();

        tokio::spawn(async move {
            sleep(Duration::from_millis(5000)).await;
            shutdown.start_shutdown();
        });

        let result = monitor.run().await;
        assert!(
            result.is_err_and(|e| matches!(e, MonitorError::ExitError(_))),
            "Monitor did not return an error as expected"
        );
    }

    #[tokio::test]
    async fn test_monitor_register_multiple() {
        let mut backend = JsonStorage::new_temp().unwrap();

        for i in 0..10 {
            backend.push(i).await.unwrap();
        }

        let monitor: Monitor = Monitor::new();

        let monitor = monitor.on_event(|wrk, e| {
            println!("{:?}, {e:?}", wrk.name());
        });
        let b = backend.clone();
        let monitor = monitor
            .register(move |index| {
                WorkerBuilder::new(format!("worker0-{}", index))
                    .backend(backend.clone())
                    .layer(ConcurrencyLimitLayer::new(1))
                    .build(
                        move |request: i32, id: TaskId, w: WorkerContext| async move {
                            println!("{id:?}, {}", w.name());
                            tokio::time::sleep(Duration::from_secs(index as u64)).await;
                            Ok::<_, io::Error>(request)
                        },
                    )
            })
            .register(move |index| {
                WorkerBuilder::new(format!("worker1-{}", index))
                    .backend(b.clone())
                    .layer(ConcurrencyLimitLayer::new(1))
                    .build(
                        move |request: i32, id: TaskId, w: WorkerContext| async move {
                            println!("{id:?}, {}", w.name());
                            tokio::time::sleep(Duration::from_secs(index as u64)).await;
                            Ok::<_, io::Error>(request)
                        },
                    )
            });
        assert_eq!(monitor.workers.len(), 2);
        let shutdown = monitor.shutdown.clone();

        tokio::spawn(async move {
            sleep(Duration::from_millis(5000)).await;
            shutdown.start_shutdown();
        });

        let result = monitor.run().await;
        assert!(result.is_ok());
    }
}
