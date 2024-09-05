use std::{
    any::Any,
    fmt::{self, Debug, Formatter},
    sync::{Arc, RwLock},
};

use futures::{future::BoxFuture, Future, FutureExt};
use serde::Serialize;
use tower::{Layer, Service};
mod shutdown;

use crate::{
    error::BoxDynError,
    executor::Executor,
    request::Request,
    worker::{Context, Event, Ready, Worker},
    Backend,
};

use self::shutdown::Shutdown;

/// A monitor for coordinating and managing a collection of workers.
pub struct Monitor<E> {
    workers: Vec<Worker<Context<E>>>,
    executor: E,
    context: MonitorContext,
    terminator: Option<BoxFuture<'static, ()>>,
}

/// The internal context of a [Monitor]
/// Usually shared with multiple workers
#[derive(Clone)]
pub struct MonitorContext {
    #[allow(clippy::type_complexity)]
    event_handler: Arc<RwLock<Option<Box<dyn Fn(Worker<Event>) + Send + Sync>>>>,
    shutdown: Shutdown,
}

impl fmt::Debug for MonitorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MonitorContext")
            .field("events", &self.event_handler.type_id())
            .field("shutdown", &"[Shutdown]")
            .finish()
    }
}

impl MonitorContext {
    fn new() -> MonitorContext {
        Self {
            event_handler: Arc::default(),
            shutdown: Shutdown::new(),
        }
    }

    /// Get the shutdown handle
    pub fn shutdown(&self) -> &Shutdown {
        &self.shutdown
    }
    /// Get the events handle
    pub fn notify(&self, event: Worker<Event>) {
        let _ = self
            .event_handler
            .as_ref()
            .read()
            .map(|caller| caller.as_ref().map(|caller| caller(event)));
    }
}

impl<E> Debug for Monitor<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Monitor")
            .field("shutdown", &"[Graceful shutdown listener]")
            .field("workers", &self.workers)
            .field("executor", &std::any::type_name::<E>())
            .finish()
    }
}

impl<E: Executor + Clone + Send + 'static + Sync> Monitor<E> {
    /// Registers a single instance of a [Worker]
    pub fn register<
        Req: Send + Sync + 'static,
        S: Service<Request<Req, Ctx>> + Send + 'static,
        P: Backend<Request<Req, Ctx>, Res> + 'static,
        Res: 'static,
        Ctx: Send + Sync + 'static,
    >(
        mut self,
        worker: Worker<Ready<S, P>>,
    ) -> Self
    where
        S::Future: Send,
        S::Response: 'static + Send + Sync + Serialize,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        P::Stream: Unpin + Send + 'static,
        P::Layer: Layer<S>,
        P: Backend<Request<Req, Ctx>, Res> + 'static,
        <P::Layer as Layer<S>>::Service: Service<Request<Req, Ctx>, Response = Res>,
        <P::Layer as Layer<S>>::Service: Send,
        <<P::Layer as Layer<S>>::Service as Service<Request<Req, Ctx>>>::Future: Send,
        <<P::Layer as Layer<S>>::Service as Service<Request<Req, Ctx>>>::Error:
            Send + Into<BoxDynError> + Sync,
        S: Service<Request<Req, Ctx>, Response = Res> + Send + 'static,
        Ctx: Send + Sync + 'static,
    {
        self.workers.push(worker.with_monitor(&self));

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
    pub fn register_with_count<
        Req: Send + Sync + 'static,
        S,
        P,
        Res: 'static + Send,
        Ctx: Send + Sync + 'static,
    >(
        mut self,
        count: usize,
        worker: Worker<Ready<S, P>>,
    ) -> Self
    where
        S::Future: Send,
        S::Response: 'static + Send + Sync + Serialize,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        P::Stream: Unpin + Send + 'static,
        P::Layer: Layer<S>,
        P: Backend<Request<Req, Ctx>, Res> + 'static,
        <P::Layer as Layer<S>>::Service: Service<Request<Req, Ctx>, Response = Res>,
        <P::Layer as Layer<S>>::Service: Send,
        <<P::Layer as Layer<S>>::Service as Service<Request<Req, Ctx>>>::Future: Send,
        <<P::Layer as Layer<S>>::Service as Service<Request<Req, Ctx>>>::Error:
            Send + Into<BoxDynError> + Sync,
        S: Service<Request<Req, Ctx>, Response = Res> + Send + 'static,
        Ctx: Send + Sync + 'static,
    {
        let workers = worker.with_monitor_instances(count, &self);
        self.workers.extend(workers);
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

    pub async fn run_with_signal<S: Future<Output = std::io::Result<()>>>(
        self,
        signal: S,
    ) -> std::io::Result<()>
    where
        E: Executor + Clone + Send + 'static,
    {
        let shutdown = self.context.shutdown.clone();
        let shutdown_after = self.context.shutdown.shutdown_after(signal);
        let runner = self.run();
        futures::try_join!(shutdown_after, runner)?;
        shutdown.await;
        Ok(())
    }

    /// Runs the monitor and all its registered workers until they have all completed.
    ///
    /// # Errors
    ///
    /// If the monitor fails to shutdown gracefully, an `std::io::Error` will be returned.
    ///
    /// # Remarks
    ///
    /// If a timeout has been set using the `shutdown_timeout` method, the monitor
    /// will wait for all workers to complete up to the timeout duration before exiting.
    /// If the timeout is reached and workers have not completed, the monitor will exit forcefully.
    pub async fn run(self) -> std::io::Result<()>
    where
        E: Executor + Clone + Send + 'static,
    {
        let mut futures = Vec::new();
        for worker in self.workers {
            futures.push(worker.run().boxed());
        }
        let shutdown_future = self.context.shutdown.boxed().map(|_| ());
        if let Some(terminator) = self.terminator {
            let runner = futures::future::select(
                futures::future::join_all(futures).map(|_| ()),
                shutdown_future,
            );
            futures::join!(runner, terminator);
        } else {
            futures::join!(
                futures::future::join_all(futures).map(|_| ()),
                shutdown_future,
            );
        }
        Ok(())
    }

    /// Handles events emitted
    pub fn on_event<F: Fn(Worker<Event>) + Send + Sync + 'static>(self, f: F) -> Self {
        let _ = self.context.event_handler.write().map(|mut res| {
            let _ = res.insert(Box::new(f));
        });
        self
    }
    /// Get the current executor
    pub fn executor(&self) -> &E {
        &self.executor
    }

    pub(crate) fn context(&self) -> &MonitorContext {
        &self.context
    }
}

impl<E: Default> Default for Monitor<E> {
    fn default() -> Self {
        Self {
            executor: E::default(),
            context: MonitorContext::new(),
            workers: Vec::new(),
            terminator: None,
        }
    }
}

impl<E> Monitor<E> {
    /// Creates a new monitor instance.
    ///
    /// # Returns
    ///
    /// A new monitor instance, with an empty collection of workers.
    pub fn new() -> Self
    where
        E: Default,
    {
        Self::new_with_executor(E::default())
    }
    /// Creates a new monitor instance with an executor
    ///
    /// # Returns
    ///
    /// A new monitor instance, with an empty collection of workers.
    pub fn new_with_executor(executor: E) -> Self {
        Self {
            context: MonitorContext::new(),
            workers: Vec::new(),
            executor,
            terminator: None,
        }
    }

    /// Sets a custom executor for the monitor, allowing the usage of another runtime apart from Tokio.
    /// The executor must implement the `Executor` trait.
    pub fn set_executor<NE: Executor>(self, executor: NE) -> Monitor<NE> {
        if !self.workers.is_empty() {
            panic!("Tried changing executor when already loaded some workers");
        }
        Monitor {
            context: self.context,
            workers: Vec::new(),
            executor,
            terminator: self.terminator,
        }
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
        TestExecutor,
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
        let monitor: Monitor<TestExecutor> = Monitor::new();
        let monitor = monitor.register(worker);
        let shutdown = monitor.context.shutdown.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(1500)).await;
            shutdown.shutdown();
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
        let monitor: Monitor<TestExecutor> = Monitor::new();
        let monitor = monitor.on_event(|e| {
            println!("{e:?}");
        });
        let monitor = monitor.register_with_count(5, worker);
        assert_eq!(monitor.workers.len(), 5);
        let shutdown = monitor.context.shutdown.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(1000)).await;
            shutdown.shutdown();
        });

        let result = monitor.run().await;
        sleep(Duration::from_millis(1000)).await;
        assert!(result.is_ok());
    }
}
