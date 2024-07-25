use self::stream::WorkerStream;
use crate::error::{BoxDynError, Error};
use crate::executor::Executor;
use crate::layers::extensions::Data;
use crate::monitor::{Monitor, MonitorContext};
use crate::notify::Notify;
use crate::poller::FetchNext;
use crate::request::Request;
use crate::service_fn::FromData;
use crate::Backend;
use futures::future::Shared;
use futures::{Future, FutureExt};
use pin_project_lite::pin_project;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fmt::{self, Display};
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskCtx, Poll, Waker};
use thiserror::Error;
use tower::{Layer, Service, ServiceBuilder, ServiceExt};

mod buffer;
mod stream;

pub use buffer::service::Buffer;

// By default a worker starts 3 futures, one for polling, one for worker stream and the other for consuming.
const WORKER_FUTURES: usize = 3;

type WorkerNotify<T> = Notify<Worker<FetchNext<T>>>;

/// A worker name wrapper usually used by Worker builder
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerId {
    name: String,
    instance: Option<usize>,
}

impl FromStr for WorkerId {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts: Vec<&str> = s.rsplit('-').collect();

        match parts.len() {
            1 => Ok(WorkerId {
                name: parts[0].to_string(),
                instance: None,
            }),
            _ => {
                let instance_str = parts[0];
                match instance_str.parse() {
                    Ok(instance) => {
                        let remainder = &mut parts[1..];
                        remainder.reverse();
                        let name = remainder.join("-");
                        Ok(WorkerId {
                            name: name.to_string(),
                            instance: Some(instance),
                        })
                    }
                    Err(_) => Ok(WorkerId {
                        name: {
                            let all = &mut parts[0..];
                            all.reverse();
                            all.join("-")
                        },
                        instance: None,
                    }),
                }
            }
        }
    }
}

impl FromData for WorkerId {}

impl Display for WorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())?;
        if let Some(instance) = self.instance {
            f.write_str("-")?;
            f.write_str(&instance.to_string())?;
        }
        Ok(())
    }
}

impl WorkerId {
    /// Build a new worker ref
    pub fn new<T: AsRef<str>>(name: T) -> Self {
        Self {
            name: name.as_ref().to_string(),
            instance: None,
        }
    }

    /// Build a new worker ref
    pub fn new_with_instance<T: AsRef<str>>(name: T, instance: usize) -> Self {
        Self {
            name: name.as_ref().to_string(),
            instance: Some(instance),
        }
    }
    /// Get the name of the worker
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the name of the worker
    pub fn instance(&self) -> &Option<usize> {
        &self.instance
    }
}

/// Events emitted by a worker
#[derive(Debug)]
pub enum Event {
    /// Worker started
    Start,
    /// Worker got a job
    Engage,
    /// Worker is idle, stream has no new request for now
    Idle,
    /// Worker encountered an error
    Error(BoxDynError),
    /// Worker stopped
    Stop,
    /// Worker completed all pending tasks
    Exit,
}

/// Possible errors that can occur when starting a worker.
#[derive(Error, Debug, Clone)]
pub enum WorkerError {
    /// An error occurred while processing a job.
    #[error("Failed to process job: {0}")]
    ProcessingError(String),
    /// An error occurred in the worker's service.
    #[error("Service error: {0}")]
    ServiceError(String),
    /// An error occurred while trying to start the worker.
    #[error("Failed to start worker: {0}")]
    StartError(String),
}

/// A worker that is ready for running
#[derive(Debug)]
pub struct Ready<S, P> {
    service: S,
    backend: P,
}
impl<S, P> Ready<S, P> {
    /// Build a worker that is ready for execution
    pub fn new(service: S, poller: P) -> Self {
        Ready {
            service,
            backend: poller,
        }
    }
}

/// Represents a generic [Worker] that can be in many different states
#[derive(Debug, Clone)]
pub struct Worker<T> {
    id: WorkerId,
    state: T,
}

impl<T> Worker<T> {
    /// Create a new worker instance
    pub fn new(id: WorkerId, state: T) -> Self {
        Self { id, state }
    }

    /// Get the inner state
    pub fn inner(&self) -> &T {
        &self.state
    }

    /// Get the worker id
    pub fn id(&self) -> &WorkerId {
        &self.id
    }
}

impl<T> Deref for Worker<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl<T> DerefMut for Worker<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl<E: Executor + Clone + Send + 'static> Worker<Context<E>> {
    /// Start a worker
    pub async fn run(self) {
        let instance = self.instance;
        let monitor = self.state.context.clone();
        self.state.running.store(true, Ordering::Relaxed);
        self.state.await;
        if let Some(ctx) = monitor.as_ref() {
            ctx.notify(Worker {
                state: Event::Exit,
                id: WorkerId::new_with_instance(self.id.name, instance),
            });
        };
    }
}

impl<S, P> Worker<Ready<S, P>> {
    /// Start a worker with a custom executor
    pub fn with_executor<E, J, Res: 'static>(self, executor: E) -> Worker<Context<E>>
    where
        S: Service<Request<J>> + Send + 'static,
        P: Backend<Request<J>, Res> + 'static,
        J: Send + 'static + Sync,
        S::Future: Send,
        S::Response: 'static + Send + Sync + Serialize,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        <P as Backend<Request<J>, Res>>::Stream: Unpin + Send + 'static,
        E: Executor + Clone + Send + 'static + Sync,
        P::Layer: Layer<S>,
        <<P as Backend<Request<J>, Res>>::Layer as Layer<S>>::Service: Service<Request<J>, Response = Res> + 'static,
        <<P as Backend<Request<J>, Res>>::Layer as Layer<S>>::Service: Send,
        <<<P as Backend<Request<J>, Res>>::Layer as Layer<S>>::Service as Service<Request<J>>>::Future:
            Send,
        <<<P as Backend<Request<J>, Res>>::Layer as Layer<S>>::Service as Service<Request<J>>>::Error:
            Send + std::error::Error + Sync,
    {
        let notifier = Notify::new();
        let service = self.state.service;
        let backend = self.state.backend;
        let poller = backend
            .poll::<<<P as Backend<Request<J>, Res>>::Layer as Layer<S>>::Service>(self.id.clone());
        let polling = poller.heartbeat.shared();
        let default_layer = poller.layer;
        let service = default_layer.layer(service);
        let worker_stream = WorkerStream::new(poller.stream, notifier.clone())
            .into_future()
            .shared();
        Self::build_worker_instance(
            WorkerId::new(self.id.name()),
            service,
            executor.clone(),
            notifier.clone(),
            polling.clone(),
            worker_stream.clone(),
            None,
        )
    }

    /// Run as a monitored worker
    pub fn with_monitor<E, J, Res: 'static>(self, monitor: &Monitor<E>) -> Worker<Context<E>>
    where
        S: Service<Request<J>> + Send + 'static,
        P: Backend<Request<J>, Res> + 'static,
        J: Send + 'static + Sync,
        S::Future: Send,
        S::Response: 'static + Send + Sync + Serialize,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        <P as Backend<Request<J>, Res>>::Stream: Unpin + Send + 'static,
        E: Executor + Clone + Send + 'static + Sync,
        P::Layer: Layer<S>,
        <<P as Backend<Request<J>, Res>>::Layer as Layer<S>>::Service: Service<Request<J>, Response = Res>,
        <<P as Backend<Request<J>, Res>>::Layer as Layer<S>>::Service: Send,
        <<<P as Backend<Request<J>, Res>>::Layer as Layer<S>>::Service as Service<
            Request<J>,
        >>::Future: Send,
        <<<P as Backend<Request<J>, Res>>::Layer as Layer<S>>::Service as Service<
            Request<J>,
        >>::Error: Send + Into<BoxDynError> + Sync,
    {
        let notifier = Notify::new();
        let service = self.state.service;
        let backend = self.state.backend;
        let executor = monitor.executor().clone();
        let context = monitor.context().clone();
        let poller = backend
            .poll::<<<P as Backend<Request<J>, Res>>::Layer as Layer<S>>::Service>(self.id.clone());
        let default_layer = poller.layer;
        let service = default_layer.layer(service);
        let polling = poller.heartbeat.shared();
        let worker_stream = WorkerStream::new(poller.stream, notifier.clone())
            .into_future()
            .shared();
        Self::build_worker_instance(
            WorkerId::new(self.id.name()),
            service,
            executor.clone(),
            notifier.clone(),
            polling.clone(),
            worker_stream.clone(),
            Some(context.clone()),
        )
    }

    /// Run a specified amounts of instances
    pub fn with_monitor_instances<E, J, Res: 'static + Send>(
        self,
        instances: usize,
        monitor: &Monitor<E>,
    ) -> Vec<Worker<Context<E>>>
    where
        S: Service<Request<J>> + Send + 'static,
        P: Backend<Request<J>, Res> + 'static,
        J: Send + 'static + Sync,
        S::Future: Send,
        S::Response: 'static + Send + Sync + Serialize,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        <P as Backend<Request<J>, Res>>::Stream: Unpin + Send + 'static,
        E: Executor + Clone + Send + 'static + Sync,
        P::Layer: Layer<S>,
        <<P as Backend<Request<J>, Res>>::Layer as Layer<S>>::Service: Service<Request<J>, Response = Res>,
        <<P as Backend<Request<J>, Res>>::Layer as Layer<S>>::Service: Send,
        <<<P as Backend<Request<J>, Res>>::Layer as Layer<S>>::Service as Service<
            Request<J>,
        >>::Future: Send,
        <<<P as Backend<Request<J>, Res>>::Layer as Layer<S>>::Service as Service<
            Request<J>,
        >>::Error: Send + Into<BoxDynError> + Sync,
    {
        let notifier = Notify::new();
        let service = self.state.service;
        let backend = self.state.backend;
        let executor = monitor.executor().clone();
        let context = monitor.context().clone();
        let poller = backend
            .poll::<<<P as Backend<Request<J>, Res>>::Layer as Layer<S>>::Service>(self.id.clone());
        let default_layer = poller.layer;
        let service = default_layer.layer(service);
        let (service, poll_worker) = Buffer::pair(service, instances);
        let polling = poller.heartbeat.shared();
        let worker_stream = WorkerStream::new(poller.stream, notifier.clone())
            .into_future()
            .shared();
        let mut workers = Vec::new();

        executor.spawn(poll_worker);

        for instance in 0..instances {
            workers.push(Self::build_worker_instance(
                WorkerId::new_with_instance(self.id.name(), instance),
                service.clone(),
                executor.clone(),
                notifier.clone(),
                polling.clone(),
                worker_stream.clone(),
                Some(context.clone()),
            ));
        }

        workers
    }

    /// Run specified worker instances via a specific executor
    pub fn with_executor_instances<E, J, Res: 'static>(
        self,
        instances: usize,
        executor: E,
    ) -> Vec<Worker<Context<E>>>
    where
        S: Service<Request<J>, Response = Res> + Send + 'static,
        P: Backend<Request<J>, Res> + 'static,
        J: Send + 'static + Sync,
        S::Future: Send,
        S::Response: 'static + Send + Sync + Serialize,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        <P as Backend<Request<J>, Res>>::Stream: Unpin + Send + 'static,
        E: Executor + Clone + Send + 'static + Sync,
        P::Layer: Layer<S>,
        <<P as Backend<Request<J>, Res>>::Layer as Layer<S>>::Service: Service<Request<J>, Response = Res>,
        <<P as Backend<Request<J>, Res>>::Layer as Layer<S>>::Service: Send,
        <<<P as Backend<Request<J>, Res>>::Layer as Layer<S>>::Service as Service<
            Request<J>,
        >>::Future: Send,
        <<<P as Backend<Request<J>, Res>>::Layer as Layer<S>>::Service as Service<
            Request<J>,
        >>::Error: Send + Into<BoxDynError> + Sync,
    {
        let worker_id = self.id.clone();
        let notifier = Notify::new();
        let service = self.state.service;
        let (service, poll_worker) = Buffer::pair(service, instances);
        let backend = self.state.backend;
        let poller = backend.poll::<S>(worker_id.clone());
        let polling = poller.heartbeat.shared();
        let worker_stream = WorkerStream::new(poller.stream, notifier.clone())
            .into_future()
            .shared();
        executor.spawn(poll_worker);
        let mut workers = Vec::new();
        for instance in 0..instances {
            workers.push(Self::build_worker_instance(
                WorkerId::new_with_instance(self.id.name(), instance),
                service.clone(),
                executor.clone(),
                notifier.clone(),
                polling.clone(),
                worker_stream.clone(),
                None,
            ));
        }
        workers
    }

    pub(crate) fn build_worker_instance<LS, J, E, Res>(
        id: WorkerId,
        service: LS,
        executor: E,
        notifier: WorkerNotify<Result<Option<Request<J>>, Error>>,
        polling: Shared<impl Future<Output = ()> + Send + 'static>,
        worker_stream: Shared<impl Future<Output = ()> + Send + 'static>,
        context: Option<MonitorContext>,
    ) -> Worker<Context<E>>
    where
        LS: Service<Request<J>, Response = Res> + Send + 'static,
        LS::Future: Send + 'static,
        LS::Response: 'static,
        LS::Error: Send + Sync + Into<BoxDynError> + 'static,
        P: Backend<Request<J>, Res>,
        E: Executor + Send + Clone + 'static + Sync,
        J: Sync + Send + 'static,
        S: 'static,
        P: 'static,
    {
        let instance = id.instance.unwrap_or_default();
        let ctx = Context {
            context,
            executor,
            instance,
            running: Arc::default(),
            task_count: Arc::default(),
            wakers: Arc::default(),
        };
        let worker = Worker { id, state: ctx };

        let fut = Self::build_instance(instance, service, worker.clone(), notifier);

        worker.spawn(fut);
        worker.spawn(polling);
        worker.spawn(worker_stream);
        worker
    }

    pub(crate) async fn build_instance<LS, J, E, Res>(
        instance: usize,
        service: LS,
        worker: Worker<Context<E>>,
        notifier: WorkerNotify<Result<Option<Request<J>>, Error>>,
    ) where
        LS: Service<Request<J>, Response = Res> + Send + 'static,
        LS::Future: Send + 'static,
        LS::Response: 'static,
        LS::Error: Send + Sync + Into<BoxDynError> + 'static,
        P: Backend<Request<J>, Res>,
        E: Executor + Send + Clone + 'static + Sync,
    {
        if let Some(ctx) = worker.state.context.as_ref() {
            ctx.notify(Worker {
                state: Event::Start,
                id: WorkerId::new_with_instance(worker.id.name(), instance),
            });
        };
        let worker_layers = ServiceBuilder::new()
            .layer(Data::new(worker.id.clone()))
            .layer(Data::new(worker.state.clone()));
        let mut service = worker_layers.service(service);
        worker.running.store(true, Ordering::Relaxed);
        let worker_id = worker.id().clone();
        loop {
            if worker.is_shutting_down() {
                if let Some(ctx) = worker.state.context.as_ref() {
                    ctx.notify(Worker {
                        state: Event::Stop,
                        id: WorkerId::new_with_instance(worker.id.name(), instance),
                    });
                };
                break;
            }
            match service.ready().await {
                Ok(service) => {
                    let (sender, receiver) = async_oneshot::oneshot();
                    let res = notifier.notify(Worker {
                        id: WorkerId::new_with_instance(worker.id.name(), instance),
                        state: FetchNext::new(sender),
                    });

                    if res.is_ok() {
                        match receiver.await {
                            Ok(Ok(Some(req))) => {
                                let fut = service.call(req);
                                let worker_id = worker_id.clone();
                                let state = worker.state.clone();
                                worker.spawn(fut.map(move |res| {
                                    if let Err(e) = res {
                                        if let Some(ctx) = state.context.as_ref() {
                                            ctx.notify(Worker {
                                                state: Event::Error(e.into()),
                                                id: WorkerId::new_with_instance(
                                                    worker_id.name(),
                                                    instance,
                                                ),
                                            });
                                        };
                                    }
                                }));
                            }
                            Ok(Err(e)) => {
                                if let Some(ctx) = worker.state.context.as_ref() {
                                    ctx.notify(Worker {
                                        state: Event::Error(Box::new(e)),
                                        id: WorkerId::new_with_instance(worker.id.name(), instance),
                                    });
                                };
                            }
                            Ok(Ok(None)) => {
                                if let Some(ctx) = worker.state.context.as_ref() {
                                    ctx.notify(Worker {
                                        state: Event::Idle,
                                        id: WorkerId::new_with_instance(worker.id.name(), instance),
                                    });
                                };
                            }
                            Err(_) => {
                                // Listener was dropped, no need to notify
                            }
                        }
                    }
                }
                Err(e) => {
                    if let Some(ctx) = worker.state.context.as_ref() {
                        ctx.notify(Worker {
                            state: Event::Error(e.into()),
                            id: WorkerId::new_with_instance(worker.id.name(), instance),
                        });
                    };
                }
            }
        }
    }
}
/// Stores the Workers context
#[derive(Clone)]
pub struct Context<E> {
    context: Option<MonitorContext>,
    executor: E,
    task_count: Arc<AtomicUsize>,
    wakers: Arc<Mutex<Vec<Waker>>>,
    running: Arc<AtomicBool>,
    instance: usize,
}

impl<E> fmt::Debug for Context<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerContext")
            .field("shutdown", &["Shutdown handle"])
            .field("instance", &self.instance)
            .finish()
    }
}

pin_project! {
    struct Tracked<F, E> {
        worker: Context<E>,
        #[pin]
        task: F,
    }
}

impl<F: Future, E: Executor + Send + Clone + 'static> Future for Tracked<F, E> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskCtx<'_>) -> Poll<F::Output> {
        let this = self.project();

        match this.task.poll(cx) {
            res @ Poll::Ready(_) => {
                this.worker.end_task();
                res
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<E: Executor + Send + 'static + Clone> Context<E> {
    /// Allows spawning of futures that will be gracefully shutdown by the worker
    pub fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        self.executor.spawn(self.track(future));
    }

    fn track<F: Future<Output = ()>>(&self, task: F) -> Tracked<F, E> {
        self.start_task();
        Tracked {
            worker: self.clone(),
            task,
        }
    }

    /// Calling this function triggers shutting down the worker
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
        self.wake()
    }

    fn start_task(&self) {
        self.task_count.fetch_add(1, Ordering::Relaxed);
    }

    fn end_task(&self) {
        if self.task_count.fetch_sub(1, Ordering::Relaxed) == WORKER_FUTURES {
            self.wake();
        }
    }

    pub(crate) fn wake(&self) {
        if let Ok(mut wakers) = self.wakers.lock() {
            for waker in wakers.drain(..) {
                waker.wake();
            }
        }
    }

    /// Returns whether the worker is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Is the shutdown token called
    pub fn is_shutting_down(&self) -> bool {
        self.context
            .as_ref()
            .map(|s| s.shutdown().is_shutting_down())
            .unwrap_or(false)
    }

    fn add_waker(&self, cx: &mut TaskCtx<'_>) {
        if let Ok(mut wakers) = self.wakers.lock() {
            if !wakers.iter().any(|w| w.will_wake(cx.waker())) {
                wakers.push(cx.waker().clone());
            }
        }
    }
}

impl<E: Executor + Send + Clone + 'static + Sync> FromData for Context<E> {}

impl<E: Executor + Send + Clone + 'static> Future for Context<E> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut TaskCtx<'_>) -> Poll<()> {
        let running = self.is_running();
        let task_count = self.task_count.load(Ordering::Relaxed);
        if self.is_shutting_down() || !running {
            if task_count <= WORKER_FUTURES {
                self.stop();
                Poll::Ready(())
            } else {
                self.add_waker(cx);
                Poll::Pending
            }
        } else {
            self.add_waker(cx);
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io, ops::Deref, sync::atomic::AtomicUsize, time::Duration};

    #[derive(Debug, Clone)]
    struct TokioTestExecutor;

    impl Executor for TokioTestExecutor {
        fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
            tokio::spawn(future);
        }
    }

    use crate::{
        builder::{WorkerBuilder, WorkerFactoryFn},
        layers::extensions::Data,
        memory::MemoryStorage,
        mq::MessageQueue,
    };

    use super::*;

    const ITEMS: u32 = 100;

    #[test]
    fn it_parses_worker_names() {
        assert_eq!(
            WorkerId::from_str("worker").unwrap(),
            WorkerId {
                instance: None,
                name: "worker".to_string()
            }
        );
        assert_eq!(
            WorkerId::from_str("worker-0").unwrap(),
            WorkerId {
                instance: Some(0),
                name: "worker".to_string()
            }
        );
        assert_eq!(
            WorkerId::from_str("complex&*-worker-name-0").unwrap(),
            WorkerId {
                instance: Some(0),
                name: "complex&*-worker-name".to_string()
            }
        );
    }

    #[tokio::test]
    async fn it_works() {
        let in_memory = MemoryStorage::new();
        let mut handle = in_memory.clone();

        tokio::spawn(async move {
            for i in 0..ITEMS {
                handle.enqueue(i).await.unwrap();
            }
        });

        #[derive(Clone, Debug, Default)]
        struct Count(Arc<AtomicUsize>);

        impl Deref for Count {
            type Target = Arc<AtomicUsize>;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        async fn task(job: u32, count: Data<Count>) -> Result<(), io::Error> {
            count.fetch_add(1, Ordering::Relaxed);
            if job == ITEMS - 1 {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Ok(())
        }
        let worker = WorkerBuilder::new("rango-tango")
            // .chain(|svc| svc.timeout(Duration::from_millis(500)))
            .data(Count::default())
            .backend(in_memory);
        let worker = worker.build_fn(task);
        let worker = worker.with_executor(TokioTestExecutor);
        let w = worker.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            w.stop();
        });
        worker.run().await;
    }
}
