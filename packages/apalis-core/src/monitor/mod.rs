use std::{
    any::Any,
    fmt::{self, Debug, Formatter},
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context as TaskCtx, Poll},
};

use futures::{Future, FutureExt};
use pin_project_lite::pin_project;
use tower::Service;
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
        J: Send + Sync + 'static,
        S: Service<Request<J>> + Send + 'static + Clone,
        P: Backend<Request<J>> + 'static,
    >(
        mut self,
        worker: Worker<Ready<S, P>>,
    ) -> Self
    where
        S::Future: Send,
        S::Response: 'static,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        <P as Backend<Request<J>>>::Stream: Unpin + Send + 'static,
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
        J: Send + Sync + 'static,
        S: Service<Request<J>> + Send + 'static + Clone,
        P: Backend<Request<J>> + 'static,
    >(
        mut self,
        count: usize,
        worker: Worker<Ready<S, P>>,
    ) -> Self
    where
        S::Future: Send,
        S::Response: 'static,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        <P as Backend<Request<J>>>::Stream: Unpin + Send + 'static,
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
    /// If the timeout is reached and workers have not completed, the monitor will log a warning
    /// message and exit forcefully.
    pub async fn run(self) -> std::io::Result<()>
    where
        E: Executor + Clone + Send + 'static,
    {
        let mut futures = Vec::new();
        for worker in self.workers {
            futures.push(worker.run().boxed());
        }
        let shutdown_future = self.context.shutdown.boxed();

        futures::join!(
            futures::future::join_all(futures).map(|_| ()),
            shutdown_future,
        );
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

pin_project! {
    struct EventHandlerFuture<F, N> {
        #[pin]
        fut: F,
        shutdown: Shutdown,
        #[pin]
        notified: N
    }
}
impl<F: Future<Output = ()>, N: Future<Output = ()>> Future for EventHandlerFuture<F, N> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut TaskCtx<'_>) -> Poll<()> {
        let this = self.project();
        let shutdown = this.shutdown;
        if shutdown.is_shutting_down() {
            this.notified.poll(cx)
        } else {
            this.fut.poll(cx)
        }
    }
}

impl<E: Default> Default for Monitor<E> {
    fn default() -> Self {
        Self {
            executor: E::default(),
            context: MonitorContext::new(),
            workers: Vec::new(),
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
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io, time::Duration};

    use tokio::time::sleep;

    use crate::{
        builder::{WorkerBuilder, WorkerFactory},
        memory::MemoryStorage,
        monitor::Monitor,
        mq::MessageQueue,
        request::Request,
        TestExecutor,
    };

    #[tokio::test]
    async fn it_works() {
        let backend = MemoryStorage::new();
        let handle = backend.clone();

        tokio::spawn(async move {
            for i in 0..10 {
                handle.enqueue(i).await.unwrap();
            }
        });
        let service = tower::service_fn(|request: Request<u32>| async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok::<_, io::Error>(request)
        });
        let worker = WorkerBuilder::new("rango-tango")
            .source(backend)
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
        let handle = backend.clone();

        tokio::spawn(async move {
            for i in 0..1000 {
                handle.enqueue(i).await.unwrap();
            }
        });
        let service = tower::service_fn(|request: Request<u32>| async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok::<_, io::Error>(request)
        });
        let worker = WorkerBuilder::new("rango-tango")
            .source(backend)
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
