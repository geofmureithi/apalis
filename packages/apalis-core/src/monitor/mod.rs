use std::{
    any::Any,
    fmt::{self, Debug, Formatter},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{Future, FutureExt, StreamExt};
use pin_project_lite::pin_project;
use tower::Service;
pub mod shutdown;

use crate::{
    error::BoxDynError,
    executor::Executor,
    notify::Notify,
    poller::Ready,
    request::Request,
    worker::{ReadyWorker, Worker, WorkerContext, WorkerEvent},
    Backend,
};

use self::shutdown::Shutdown;

/// A monitor for coordinating and managing a collection of workers.
pub struct Monitor<E> {
    workers: Vec<Worker<WorkerContext<E>>>,
    pub(crate) executor: E,
    pub(crate) context: MonitorContext,
}

/// The internal context of a [Monitor]
/// Usually shared with multiple workers
#[derive(Clone)]
pub struct MonitorContext {
    event_handler: Option<Arc<Box<dyn Fn(Worker<WorkerEvent>) + Send + Sync>>>,
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
            event_handler: None,
            shutdown: Shutdown::new(),
        }
    }

    /// Get the shutdown handle
    pub fn shutdown(&self) -> &Shutdown {
        &self.shutdown
    }
    /// Get the events handle
    pub fn notify(&self, event: Worker<WorkerEvent>) {
        self.event_handler.as_ref().map(|caller| caller(event));
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
        worker: Worker<ReadyWorker<S, P>>,
    ) -> Self
    where
        S::Future: Send,
        S::Response: 'static,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        <P as Backend<Request<J>>>::Stream: Unpin + Send + 'static,
    {
        self.workers.push(worker.run_monitored(&self));

        self
    }

    pub fn register_with_count<
        J: Send + Sync + 'static,
        S: Service<Request<J>> + Send + 'static + Clone,
        P: Backend<Request<J>> + 'static,
    >(
        mut self,
        count: usize,
        worker: Worker<ReadyWorker<S, P>>,
    ) -> Self
    where
        S::Future: Send,
        S::Response: 'static,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        <P as Backend<Request<J>>>::Stream: Unpin + Send + 'static,
    {
        let workers = worker.run_instances_monitored(count, &self);
        self.workers.extend(workers);
        self
    }

    // /// Registers a worker with the monitor.
    // ///
    // /// # Arguments
    // ///
    // /// * `worker` - The worker to register.
    // ///
    // /// # Returns
    // ///
    // /// The monitor instance, with the worker added to the collection.
    // pub fn register<
    //     Strm,
    //     Serv: Service<Request<J>>,
    //     J: Job + 'static,
    //     W: Worker<J, Service = Serv, Source = Strm> + 'static,
    // >(
    //     mut self,
    //     worker: W,
    // ) -> Self
    // where
    //     <Serv as Service<Request<J>>>::Future: std::marker::Send,
    // {
    //     let shutdown = self.shutdown.clone();
    //     let worker_id = worker.id();
    //     // self.executor.spawn(
    //     //     self.shutdown.graceful(
    //     //         worker
    //     //             .start(WorkerContext {
    //     //                 shutdown,
    //     //                 executor: self.executor.clone(),
    //     //                 worker_id: worker_id.clone(),
    //     //             })
    //     //             .map(|_| ()),
    //     //     ),
    //     // );
    //     // self.workers.push(worker_id);
    //     self
    // }

    // /// Registers multiple workers with the monitor.
    // ///
    // /// # Arguments
    // ///
    // /// * `count` - The number of workers to register.
    // /// * `caller` - A function that returns a new worker instance for each index.
    // ///
    // /// # Returns
    // ///
    // /// The monitor instance, with all workers added to the collection.
    // ///
    // pub fn register_with_count<
    //     Strm,
    //     Serv: Service<Request<J>>,
    //     J: Job + 'static,
    //     W: Worker<J, Service = Serv, Source = Strm> + 'static,
    //     Call: Fn(u16) -> W,
    // >(
    //     mut self,
    //     count: u16,
    //     caller: Call,
    // ) -> Self
    // where
    //     <Serv as Service<Request<J>>>::Future: std::marker::Send,
    // {
    //     for index in 0..count {
    //         let worker = caller(index);
    //         self = self.register(worker);
    //     }

    //     self
    // }

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
    pub async fn run(mut self) -> std::io::Result<()>
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

    pub fn on_event<F: Fn(Worker<WorkerEvent>) + Send + Sync + 'static>(mut self, f: F) -> Self {
        if self.workers.len() > 0 {
            panic!("To listen to workers, please add the listener before registering");
        }
        self.context.event_handler = Some(Arc::new(Box::new(f)));
        self
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

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
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

impl<E: Default> Monitor<E> {
    /// Creates a new monitor instance.
    ///
    /// # Returns
    ///
    /// A new monitor instance, with an empty collection of workers.
    pub fn new() -> Self {
        Self {
            context: MonitorContext::new(),
            workers: Vec::new(),
            executor: E::default(),
        }
    }

    /// Sets a custom executor for the monitor, allowing the usage of another runtime apart from Tokio.
    /// The executor must implement the `Executor` trait.
    pub fn executor<NE: Executor>(self, executor: NE) -> Monitor<NE> {
        if self.workers.len() > 0 {
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
        worker::{Worker, WorkerEvent},
        TokioTestExecutor,
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
        let monitor: Monitor<TokioTestExecutor> = Monitor::new();
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
            // tokio::time::sleep(Duration::from_secs(1)).await;
            println!("{request:?}");
            Ok::<_, io::Error>(request)
        });
        let worker = WorkerBuilder::new("rango-tango")
            .source(backend)
            .build(service);
        let monitor: Monitor<TokioTestExecutor> = Monitor::new();
        let monitor = monitor.on_event(|e: Worker<WorkerEvent>| {
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
