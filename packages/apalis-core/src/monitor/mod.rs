use std::{
    fmt::{self, Debug, Formatter},
    sync::Arc,
};

use futures::{future::BoxFuture, Future, FutureExt};
use tower::{Layer, Service};

/// Shutdown utilities
pub mod shutdown;

use crate::{
    backend::Backend,
    error::BoxDynError,
    request::Request,
    worker::{Context, Event, EventHandler, Ready, Worker, WorkerId},
};

use self::shutdown::Shutdown;

/// A monitor for coordinating and managing a collection of workers.
pub struct Monitor {
    futures: Vec<BoxFuture<'static, ()>>,
    workers: Vec<Worker<Context>>,
    terminator: Option<BoxFuture<'static, ()>>,
    shutdown: Shutdown,
    event_handler: EventHandler,
}

impl Debug for Monitor {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Monitor")
            .field("shutdown", &"[Graceful shutdown listener]")
            .field("workers", &self.futures.len())
            .finish()
    }
}

impl Monitor {
    /// Registers a single instance of a [Worker]
    pub fn register<Req, S, P, Ctx>(mut self, mut worker: Worker<Ready<S, P>>) -> Self
    where
        S: Service<Request<Req, Ctx>> + Send + 'static,
        S::Future: Send,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        P: Backend<Request<Req, Ctx>> + Send + 'static,
        P::Stream: Unpin + Send + 'static,
        P::Layer: Layer<S> + Send,
        <P::Layer as Layer<S>>::Service: Service<Request<Req, Ctx>> + Send,
        <<P::Layer as Layer<S>>::Service as Service<Request<Req, Ctx>>>::Future: Send,
        <<P::Layer as Layer<S>>::Service as Service<Request<Req, Ctx>>>::Error:
            Send + Sync + Into<BoxDynError>,
        Req: Send + 'static,
        Ctx: Send + 'static,
    {
        worker.state.shutdown = Some(self.shutdown.clone());
        worker.state.event_handler = self.event_handler.clone();
        let runnable = worker.run();
        let handle = runnable.get_handle();
        self.workers.push(handle);
        self.futures.push(runnable.boxed());
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
    #[deprecated(
        since = "0.6.0",
        note = "Consider using the `.register` as workers now offer concurrency by default"
    )]
    pub fn register_with_count<Req, S, P, Ctx>(
        mut self,
        count: usize,
        worker: Worker<Ready<S, P>>,
    ) -> Self
    where
        S: Service<Request<Req, Ctx>> + Send + 'static + Clone,
        S::Future: Send,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        P: Backend<Request<Req, Ctx>> + Send + 'static + Clone,
        P::Stream: Unpin + Send + 'static,
        P::Layer: Layer<S> + Send,
        <P::Layer as Layer<S>>::Service: Service<Request<Req, Ctx>> + Send,
        <<P::Layer as Layer<S>>::Service as Service<Request<Req, Ctx>>>::Future: Send,
        <<P::Layer as Layer<S>>::Service as Service<Request<Req, Ctx>>>::Error:
            Send + Sync + Into<BoxDynError>,
        Req: Send + 'static,
        Ctx: Send + 'static,
    {
        for index in 0..count {
            let mut worker = worker.clone();
            let name = format!("{}-{index}", worker.id());
            worker.id = WorkerId::new(name);
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
                futures::future::join_all(self.futures)
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
            let _res = futures::join!(shutdown_after, runner); // If no terminator is provided, we wait for both the shutdown call and all workers to complete
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
            futures::future::join_all(self.futures).map(|_| shutdown.start_shutdown()),
            shutdown_future,
        );

        Ok(())
    }

    /// Handles events emitted
    pub fn on_event<F: Fn(Worker<Event>) + Send + Sync + 'static>(self, f: F) -> Self {
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
            futures: Vec::new(),
            terminator: None,
            event_handler: Arc::default(),
            workers: Vec::new(),
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
        self.with_terminator(crate::sleep(duration))
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
    use crate::test_utils::apalis_test_service_fn;
    use std::{io, time::Duration};

    use tokio::time::sleep;

    use crate::{
        builder::{WorkerBuilder, WorkerFactory},
        memory::MemoryStorage,
        monitor::Monitor,
        mq::MessageQueue,
        request::Request,
        test_message_queue,
        test_utils::TestWrapper,
    };

    test_message_queue!(MemoryStorage::new());

    #[tokio::test]
    async fn it_works_with_workers() {
        let backend = MemoryStorage::new();
        let mut handle = backend.clone();

        tokio::spawn(async move {
            for i in 0..10 {
                handle.enqueue(i).await.unwrap();
            }
        });
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
        let backend = MemoryStorage::new();
        let mut handle = backend.clone();

        tokio::spawn(async move {
            for i in 0..10 {
                handle.enqueue(i).await.unwrap();
            }
        });
        let service = tower::service_fn(|request: Request<u32, _>| async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok::<_, io::Error>(request)
        });
        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .build(service);
        let monitor: Monitor = Monitor::new();
        let monitor = monitor.on_event(|e| {
            println!("{e:?}");
        });
        let monitor = monitor.register(worker);
        assert_eq!(monitor.futures.len(), 1);
        let shutdown = monitor.shutdown.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(1000)).await;
            shutdown.start_shutdown();
        });

        let result = monitor.run().await;
        sleep(Duration::from_millis(1000)).await;
        assert!(result.is_ok());
    }
}
