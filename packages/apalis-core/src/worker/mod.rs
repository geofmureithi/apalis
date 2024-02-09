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
use tower::{Service, ServiceBuilder, ServiceExt};

mod stream;
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
        let parts: Vec<&str> = s.rsplitn(2, '-').collect();

        match parts.len() {
            1 => Ok(WorkerId {
                name: parts[0].to_string(),
                instance: None,
            }),
            2 => {
                let name = parts[1];
                let instance_str = parts[0];
                let instance = instance_str.parse().ok();
                Ok(WorkerId {
                    name: name.to_string(),
                    instance,
                })
            }
            _ => Err(()),
        }
    }
}

impl FromData for WorkerId {}

impl Display for WorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())?;
        if self.instance.is_some() {
            f.write_str("-")?;
            f.write_str(&self.instance.unwrap().to_string())?;
        }
        Ok(())
    }
}

impl WorkerId {
    /// Build a new worker ref
    pub fn new<T: AsRef<str>>(name: T) -> Self {
        Self::from_str(name.as_ref()).unwrap()
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
#[derive(Error, Debug)]
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
    pub fn with_executor<E, J>(self, executor: E) -> Worker<Context<E>>
    where
        S: Service<Request<J>> + Send + 'static + Clone,
        P: Backend<Request<J>> + 'static,
        J: Send + 'static + Sync,
        S::Future: Send,
        S::Response: 'static,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        <P as Backend<Request<J>>>::Stream: Unpin + Send + 'static,
        E: Executor + Clone + Send + 'static + Sync,
    {
        let instances: Vec<Worker<Context<E>>> = self.with_executor_instances(1, executor);
        instances.into_iter().nth(0).unwrap()
    }

    /// Run as a monitored worker
    pub fn with_monitor<E, J>(self, monitor: &Monitor<E>) -> Worker<Context<E>>
    where
        S: Service<Request<J>> + Send + 'static + Clone,
        P: Backend<Request<J>> + 'static,
        J: Send + 'static + Sync,
        S::Future: Send,
        S::Response: 'static,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        <P as Backend<Request<J>>>::Stream: Unpin + Send + 'static,
        E: Executor + Clone + Send + 'static + Sync,
    {
        let instances: Vec<Worker<Context<E>>> = self.with_monitor_instances(1, monitor);
        instances.into_iter().nth(0).unwrap()
    }

    /// Run a specified amounts of instances
    pub fn with_monitor_instances<E, J>(
        self,
        instances: usize,
        monitor: &Monitor<E>,
    ) -> Vec<Worker<Context<E>>>
    where
        S: Service<Request<J>> + Send + 'static + Clone,
        P: Backend<Request<J>> + 'static,
        J: Send + 'static + Sync,
        S::Future: Send,
        S::Response: 'static,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        <P as Backend<Request<J>>>::Stream: Unpin + Send + 'static,
        E: Executor + Clone + Send + 'static + Sync,
    {
        let notifier = Notify::new();
        let service = self.state.service;
        let backend = self.state.backend;
        let executor = monitor.executor().clone();
        let context = monitor.context().clone();
        let poller = backend.poll(self.id.clone());
        let polling = poller.heartbeat.shared();
        let worker_stream = WorkerStream::new(poller.stream, notifier.clone())
            .into_future()
            .shared();
        let mut workers = Vec::new();
        for instance in 0..instances {
            let ctx = Context {
                context: Some(context.clone()),
                executor: executor.clone(),
                instance,
                running: Arc::default(),
                task_count: Arc::default(),
                wakers: Arc::default(),
            };
            let worker = Worker {
                id: self.id.clone(),
                state: ctx.clone(),
            };
            let fut = Self::build_worker_instance(
                instance,
                service.clone(),
                worker.clone(),
                notifier.clone(),
            );

            worker.spawn(fut);
            worker.spawn(polling.clone());
            worker.spawn(worker_stream.clone());
            workers.push(worker);
        }

        workers
    }

    /// Run specified worker instances via a specific executor
    pub fn with_executor_instances<E, J>(
        self,
        instances: usize,
        executor: E,
    ) -> Vec<Worker<Context<E>>>
    where
        S: Service<Request<J>> + Send + 'static + Clone,
        P: Backend<Request<J>> + 'static,
        J: Send + 'static + Sync,
        S::Future: Send,
        S::Response: 'static,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        <P as Backend<Request<J>>>::Stream: Unpin + Send + 'static,
        E: Executor + Clone + Send + 'static + Sync,
    {
        let worker_id = self.id.clone();
        let notifier = Notify::new();
        let service = self.state.service;
        let backend = self.state.backend;
        let poller = backend.poll(worker_id.clone());
        let polling = poller.heartbeat.shared();
        let worker_stream = WorkerStream::new(poller.stream, notifier.clone())
            .into_future()
            .shared();

        let mut workers = Vec::new();
        for instance in 0..instances {
            let ctx = Context {
                context: None,
                executor: executor.clone(),
                instance,
                running: Arc::default(),
                task_count: Arc::default(),
                wakers: Arc::default(),
            };
            let worker = Worker {
                id: self.id.clone(),
                state: ctx.clone(),
            };

            let fut = Self::build_worker_instance(
                instance,
                service.clone(),
                worker.clone(),
                notifier.clone(),
            );

            worker.spawn(fut);
            worker.spawn(polling.clone());
            worker.spawn(worker_stream.clone());
            workers.push(worker);
        }
        workers
    }

    pub(crate) async fn build_worker_instance<LS, J, E>(
        instance: usize,
        service: LS,
        worker: Worker<Context<E>>,
        notifier: WorkerNotify<Result<Option<Request<J>>, Error>>,
    ) where
        LS: Service<Request<J>> + Send + 'static + Clone,
        LS::Future: Send + 'static,
        LS::Response: 'static,
        LS::Error: Send + Sync + Into<BoxDynError> + 'static,
        P: Backend<Request<J>>,
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
                    notifier
                        .notify(Worker {
                            id: WorkerId::new_with_instance(worker.id.name(), instance),
                            state: FetchNext::new(sender),
                        })
                        .unwrap();

                    match receiver.await {
                        Ok(Ok(Some(req))) => {
                            let fut = service.call(req);
                            worker.spawn(fut.map(|_| ()));
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
        for waker in self.wakers.lock().unwrap().drain(..) {
            waker.wake();
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
        let mut wakers = self.wakers.lock().unwrap();
        if !wakers.iter().any(|w| w.will_wake(cx.waker())) {
            wakers.push(cx.waker().clone());
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
            // self.wake();
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

    #[tokio::test]
    async fn it_works() {
        let backend = MemoryStorage::new();
        let handle = backend.clone();

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
            .chain(|svc| svc.timeout(Duration::from_millis(500)))
            .data(Count::default())
            .source(backend);
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
