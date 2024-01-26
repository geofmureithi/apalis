use crate::error::{BoxDynError, Error};
use crate::executor::Executor;
use crate::layers::extensions::Data;
use crate::monitor::{Monitor, MonitorContext};
use crate::notify::Notify;
use crate::poller::Ready;
use crate::Backend;
use crate::Request;
use futures::future::Shared;
use futures::{Future, FutureExt, StreamExt};
use pin_project_lite::pin_project;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fmt::{self, Display};
use std::io;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use thiserror::Error;
use tower::{Service, ServiceBuilder, ServiceExt};

use self::stream::WorkerStream;

mod stream;
// By default a worker starts 3 futures, one for polling, one for worker stream and the other for consuming.
const WORKER_FUTURES: usize = 3;

type WorkerNotify<T> = Notify<Worker<Ready<T>>>;

/// A worker name wrapper usually used by Worker builder
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerId {
    name: String,
}

impl Display for WorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl WorkerId {
    /// Build a new worker ref
    pub fn new<T: AsRef<str>>(name: T) -> Self {
        Self {
            name: name.as_ref().to_string(),
        }
    }
    /// Get the name of the worker
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Events emitted by a worker
#[derive(Debug)]
pub enum WorkerEvent {
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
pub struct ReadyWorker<S, P> {
    pub(crate) service: S,
    pub(crate) backend: P,
}
impl<S, P> ReadyWorker<S, P> {
    /// Build a worker that is ready for execution
    pub fn new(service: S, poller: P) -> Self {
        ReadyWorker {
            service,
            backend: poller,
        }
    }
}

/// Represents a generic [Worker] that can be in many different states
#[derive(Debug, Clone)]
pub struct Worker<T> {
    pub(crate) id: WorkerId,
    pub(crate) inner: T,
}

impl<T> Worker<T> {
    /// Create a new worker instance
    pub fn new(id: WorkerId, inner: T) -> Self {
        Self { id, inner }
    }
}

impl<T> Deref for Worker<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for Worker<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<E: Executor + Clone + Send + 'static> Worker<WorkerContext<E>> {
    /// Start a worker
    pub async fn run(self) {
        let monitor = self.inner.context.clone();
        self.inner.running.store(true, Ordering::SeqCst);
        self.inner.await;
        if let Some(ctx) = monitor.as_ref() {
            ctx.notify(Worker {
                inner: WorkerEvent::Exit,
                id: self.id.clone(),
            });
        };
    }
}

impl<S, P> Worker<ReadyWorker<S, P>> {
    /// Start a worker
    pub fn run_with<E, J>(self, executor: E) -> Worker<WorkerContext<E>>
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
        let instances: Vec<Worker<WorkerContext<E>>> = self.run_instances_with(1, executor);
        instances.into_iter().nth(0).unwrap()
    }

    pub fn run_monitored<E, J>(self, monitor: &Monitor<E>) -> Worker<WorkerContext<E>>
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
        let instances: Vec<Worker<WorkerContext<E>>> = self.run_instances_monitored(1, monitor);
        instances.into_iter().nth(0).unwrap()
    }

    pub fn run_instances_monitored<E, J>(
        self,
        instances: usize,
        monitor: &Monitor<E>,
    ) -> Vec<Worker<WorkerContext<E>>>
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
        let service = self.inner.service;
        let backend = self.inner.backend;
        let executor = monitor.executor.clone();
        let context = monitor.context.clone();
        let poller = backend.poll(self.id.clone());
        let polling = poller.heartbeat.shared();
        let worker_stream = WorkerStream::new(poller.stream, notifier.clone())
            .to_future()
            .shared();
        let mut workers = Vec::new();
        for instance in 0..instances {
            let ctx = WorkerContext {
                context: Some(context.clone()),
                executor: executor.clone(),
                instance,
                running: Arc::default(),
                task_count: Arc::default(),
                wakers: Arc::default(),
            };
            let worker = Worker {
                id: self.id.clone(),
                inner: ctx.clone(),
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

    pub fn run_instances_with<E, J>(
        self,
        instances: usize,
        executor: E,
    ) -> Vec<Worker<WorkerContext<E>>>
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
        let service = self.inner.service;
        let backend = self.inner.backend;
        let poller = backend.poll(worker_id.clone());
        let polling = poller.heartbeat.shared();
        let worker_stream = WorkerStream::new(poller.stream, notifier.clone())
            .to_future()
            .shared();

        let mut workers = Vec::new();
        for instance in 0..instances {
            let ctx = WorkerContext {
                context: None,
                executor: executor.clone(),
                instance,
                running: Arc::default(),
                task_count: Arc::default(),
                wakers: Arc::default(),
            };
            let worker = Worker {
                id: self.id.clone(),
                inner: ctx.clone(),
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
        worker: Worker<WorkerContext<E>>,
        notifier: WorkerNotify<Result<Option<Request<J>>, Error>>,
    ) where
        LS: Service<Request<J>> + Send + 'static + Clone,
        LS::Future: Send + 'static,
        LS::Response: 'static,
        LS::Error: Send + Sync + Into<BoxDynError> + 'static,
        P: Backend<Request<J>>,
        E: Executor + Send + Clone + 'static + Sync,
    {
        if let Some(ctx) = worker.inner.context.as_ref() {
            ctx
                .notify(Worker {
                    inner: WorkerEvent::Start,
                    id: worker.id.clone(),
                });
        };
        let worker_layers = ServiceBuilder::new().layer(Data(worker.clone()));
        let mut service = worker_layers.service(service);
        worker.running.store(true, Ordering::SeqCst);

        loop {
            if worker.is_shutting_down() {
                if let Some(ctx) = worker.inner.context.as_ref() {
                    ctx
                        .notify(Worker {
                            inner: WorkerEvent::Stop,
                            id: worker.id.clone(),
                        })
                       ;
                };
                break;
            }
            match service.ready().await {
                Ok(service) => {
                    let (sender, receiver) = async_oneshot::oneshot();
                    notifier
                        .notify(Worker {
                            id: worker.id.clone(),
                            inner: Ready::new(sender, instance),
                        });
                        
                    match receiver.await {
                        Ok(Ok(Some(req))) => {
                            let fut = service.call(req);
                            worker.spawn(fut.map(|_| ()));
                        }
                        Ok(Err(e)) => {
                            if let Some(ctx) = worker.inner.context.as_ref() {
                                ctx
                                    .notify(Worker {
                                        inner: WorkerEvent::Error(Box::new(e)),
                                        id: worker.id.clone(),
                                    });
                                    
                            };
                        }
                        Ok(Ok(None)) | Err(_) => {
                            if let Some(ctx) = worker.inner.context.as_ref() {
                                ctx.
                                    notify(Worker {
                                        inner: WorkerEvent::Idle,
                                        id: worker.id.clone(),
                                    });
                            };
                        }
                    }
                }
                Err(e) => {
                    if let Some(ctx) = worker.inner.context.as_ref() {
                        ctx
                            .notify(Worker {
                                inner: WorkerEvent::Error(e.into()),
                                id: worker.id.clone(),
                            });
                    };
                }
            }
        }
    }
}
/// Stores the Workers context
#[derive(Clone)]
pub struct WorkerContext<E> {
    pub(crate) context: Option<MonitorContext>,
    pub(crate) executor: E,
    pub(crate) task_count: Arc<AtomicUsize>,
    pub(crate) wakers: Arc<Mutex<Vec<Waker>>>,
    pub(crate) running: Arc<AtomicBool>,
    pub(crate) instance: usize,
}

impl<E> fmt::Debug for WorkerContext<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerContext")
            .field("shutdown", &["Shutdown handle"])
            .field("instance", &self.instance)
            .finish()
    }
}

pin_project! {
    struct Tracked<F, E> {
        worker: WorkerContext<E>,
        #[pin]
        task: F,
    }
}

impl<F: Future, E: Executor + Send + Clone + 'static> Future for Tracked<F, E> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
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

impl<E: Executor + Send + 'static + Clone> WorkerContext<E> {
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
        self.running.store(false, Ordering::SeqCst);
        self.wake()
    }

    fn start_task(&self) {
        self.task_count.fetch_add(1, Ordering::SeqCst);
    }

    fn end_task(&self) {
        if self.task_count.fetch_sub(1, Ordering::SeqCst) == WORKER_FUTURES {
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
        self.running.load(Ordering::SeqCst)
    }

    pub fn is_shutting_down(&self) -> bool {
        self.context
            .as_ref()
            .map(|s| s.shutdown().is_shutting_down())
            .unwrap_or(false)
    }

    fn add_waker(&self, cx: &mut Context<'_>) {
        let mut wakers = self.wakers.lock().unwrap();
        if !wakers.iter().any(|w| w.will_wake(cx.waker())) {
            wakers.push(cx.waker().clone());
        }
    }
}

impl<E: Executor + Send + Clone + 'static> Future for WorkerContext<E> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let running = self.is_running();
        let task_count = self.task_count.load(Ordering::SeqCst);
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
    use std::{ops::Deref, sync::atomic::AtomicUsize, time::Duration};

    use crate::{
        builder::{WorkerBuilder, WorkerFactory, WorkerFactoryFn},
        layers::extensions::Data,
        memory::MemoryStorage,
        mq::MessageQueue,
        service_fn::service_fn,
        TokioTestExecutor,
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
                println!("{job:?} {count:?}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Ok(())
        }
        let worker = WorkerBuilder::new("rango-tango")
            .chain(|svc| svc.timeout(Duration::from_millis(500)))
            .layer(Data(Count::default()))
            .source(backend);
        let worker = worker.build_fn(task);
        let worker = worker.run_with(TokioTestExecutor);
        let w = worker.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            w.stop();
        });
        worker.run().await;
    }
}
