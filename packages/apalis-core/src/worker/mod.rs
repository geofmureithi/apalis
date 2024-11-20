use crate::error::{BoxDynError, Error};
use crate::layers::extensions::Data;
use crate::monitor::shutdown::Shutdown;
use crate::request::Request;
use crate::Backend;
use futures::{Future, FutureExt, Stream, StreamExt};
use pin_project_lite::pin_project;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fmt::{self, Display};
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context as TaskCtx, Poll, Waker};
use thiserror::Error;
use tower::{Layer, Service, ServiceBuilder, ServiceExt};

/// A worker name wrapper usually used by Worker builder
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerId {
    name: String,
}

/// An event handler for [`Worker`]
pub type EventHandler = Arc<RwLock<Option<Box<dyn Fn(Worker<Event>) + Send + Sync>>>>;

impl FromStr for WorkerId {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(WorkerId { name: s.to_owned() })
    }
}

impl Display for WorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())?;
        Ok(())
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
pub struct Ready<S, P> {
    service: S,
    backend: P,
    pub(crate) shutdown: Option<Shutdown>,
    pub(crate) event_handler: EventHandler,
}

impl<S, P> fmt::Debug for Ready<S, P>
where
    S: fmt::Debug,
    P: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ready")
            .field("service", &self.service)
            .field("backend", &self.backend)
            .field("shutdown", &self.shutdown)
            .field("event_handler", &"...") // Avoid dumping potentially sensitive or verbose data
            .finish()
    }
}

impl<S, P> Clone for Ready<S, P>
where
    S: Clone,
    P: Clone,
{
    fn clone(&self) -> Self {
        Ready {
            service: self.service.clone(),
            backend: self.backend.clone(),
            shutdown: self.shutdown.clone(),
            event_handler: self.event_handler.clone(),
        }
    }
}

impl<S, P> Ready<S, P> {
    /// Build a worker that is ready for execution
    pub fn new(service: S, poller: P) -> Self {
        Ready {
            service,
            backend: poller,
            shutdown: None,
            event_handler: EventHandler::default(),
        }
    }
}

/// Represents a generic [Worker] that can be in many different states
#[derive(Debug, Clone)]
pub struct Worker<T> {
    pub(crate) id: WorkerId,
    pub(crate) state: T,
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

// impl WorkerRunner for

impl<S, P> Worker<Ready<S, P>> {
    fn emit(event_handler: &EventHandler, id: &WorkerId, event: Event) {
        if let Some(handler) = event_handler.read().unwrap().as_ref() {
            handler(Worker {
                id: id.clone(),
                state: event,
            })
        }
    }

    async fn poll_jobs<Svc, Stm, Req, Res, Ctx>(
        worker: Worker<Context>,
        service: &mut Svc,
        stream: &mut Stm,
        event_handler: &EventHandler,
    ) where
        Svc: Service<Request<Req, Ctx>, Response = Res> + Send + 'static,
        Stm: Stream<Item = Result<Option<Request<Req, Ctx>>, Error>> + Send + Unpin + 'static,
        Req: Send + 'static + Sync,
        Svc::Future: Send,
        Svc::Response: 'static + Send + Sync + Serialize,
        Svc::Error: Send + Sync + 'static + Into<BoxDynError>,
        Ctx: Send + 'static + Sync,
        Res: 'static,
    {
        match service.ready().await {
            Ok(service) => match stream.next().await {
                Some(Ok(Some(req))) => {
                    let fut = service.call(req);
                    let w = worker.clone();
                    let event_handler = event_handler.clone();
                    worker
                        .track(fut.map(move |res| {
                            if let Err(e) = res {
                                let error = e.into();
                                if let Some(Error::MissingData(_)) = error.downcast_ref::<Error>() {
                                    w.stop();
                                }
                                Self::emit(&event_handler, w.id(), Event::Error(error));
                            }
                        }))
                        .await;
                }
                Some(Err(e)) => {
                    Self::emit(&event_handler, worker.id(), Event::Error(Box::new(e)));
                }
                Some(Ok(None)) => {
                    Self::emit(&event_handler, worker.id(), Event::Idle);
                }
                None => {
                    // Self::emit(&event_handler, worker.id(), Event::Stop);
                    // worker.stop();
                }
            },
            Err(e) => {
                Self::emit(&event_handler, worker.id(), Event::Error(e.into()));
            }
        }
    }
    /// Start a worker
    pub async fn run<Req, Res, Ctx>(self)
    where
        S: Service<Request<Req, Ctx>, Response = Res> + Send + 'static,
        P: Backend<Request<Req, Ctx>, Res> + 'static,
        Req: Send + 'static + Sync,
        S::Future: Send,
        S::Response: 'static + Send + Sync + Serialize,
        S::Error: Send + Sync + 'static + Into<BoxDynError>,
        P::Stream: Unpin + Send + 'static,
        P::Layer: Layer<S>,
        <P::Layer as Layer<S>>::Service: Service<Request<Req, Ctx>, Response = Res> + Send,
        <<P::Layer as Layer<S>>::Service as Service<Request<Req, Ctx>>>::Future: Send,
        <<P::Layer as Layer<S>>::Service as Service<Request<Req, Ctx>>>::Error:
            Send + Into<BoxDynError> + Sync,
        Ctx: Send + 'static + Sync,
        Res: 'static,
    {
        let worker_id = self.id().clone();
        let ctx = Context {
            running: Arc::default(),
            task_count: Arc::default(),
            wakers: Arc::default(),
            shutdown: self.state.shutdown,
        };
        let worker = Worker {
            id: worker_id.clone(),
            state: ctx.clone(),
        };
        let backend = self.state.backend;
        let service = self.state.service;
        let emitter = self.state.event_handler;
        let poller = backend.poll::<S>(worker_id.clone());
        let mut stream = poller.stream;
        let mut heartbeat = poller.heartbeat.fuse();
        let layer = poller.layer;
        let mut service = ServiceBuilder::new()
            .layer(Data::new(worker.id.clone()))
            .layer(Data::new(worker.state.clone()))
            .layer(layer)
            .service(service);
        worker.running.store(true, Ordering::Relaxed);
        let mut worker_run = ctx.fuse();
        loop {
            let w = worker.clone();

            futures::select! {
                _ = Self::poll_jobs(worker.clone(), &mut service, &mut stream, &emitter).fuse() => {}
                _ = heartbeat => {}
                _ = worker_run => {
                    Self::emit(&emitter, w.id(), Event::Stop);
                    break;
                }
            }
        }
    }
}

/// Stores the Workers context
#[derive(Clone)]
pub struct Context {
    task_count: Arc<AtomicUsize>,
    wakers: Arc<Mutex<Vec<Waker>>>,
    running: Arc<AtomicBool>,
    shutdown: Option<Shutdown>,
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerContext")
            .field("shutdown", &["Shutdown handle"])
            .field("task_count", &self.task_count)
            .field("running", &self.running)
            .finish()
    }
}

pin_project! {
    /// A future tracked by the worker
    pub struct Tracked<F> {
        ctx: Context,
        #[pin]
        task: F,
    }
}

impl<F: Future> Future for Tracked<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskCtx<'_>) -> Poll<F::Output> {
        let this = self.project();

        match this.task.poll(cx) {
            res @ Poll::Ready(_) => {
                this.ctx.end_task();
                res
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Context {
    /// Start a task that is tracked by the worker
    pub fn track<F: Future<Output = ()>>(&self, task: F) -> Tracked<F> {
        self.start_task();
        Tracked {
            ctx: self.clone(),
            task,
        }
    }

    /// Calling this function triggers shutting down the worker while waiting for any tasks to complete
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
        self.wake()
    }

    fn start_task(&self) {
        self.task_count.fetch_add(1, Ordering::Relaxed);
        dbg!(&self.task_count);
    }

    fn end_task(&self) {
        dbg!(&self.task_count);
        if self.task_count.fetch_sub(1, Ordering::Relaxed) == 1 {
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
        self.shutdown
            .as_ref()
            .map(|s| !self.is_running() || s.is_shutting_down())
            .unwrap_or(!self.is_running())
    }

    fn add_waker(&self, cx: &mut TaskCtx<'_>) {
        if let Ok(mut wakers) = self.wakers.lock() {
            if !wakers.iter().any(|w| w.will_wake(cx.waker())) {
                wakers.push(cx.waker().clone());
            }
        }
    }
}

impl Future for Context {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut TaskCtx<'_>) -> Poll<()> {
        let running = self.is_running();
        let task_count = self.task_count.load(Ordering::Relaxed);
        dbg!(task_count, running);
        if self.is_shutting_down() || !running {
            if task_count == 0 {
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
    use std::{ops::Deref, sync::atomic::AtomicUsize};

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
                name: "worker".to_string()
            }
        );
        assert_eq!(
            WorkerId::from_str("worker-0").unwrap(),
            WorkerId {
                name: "worker-0".to_string()
            }
        );
        assert_eq!(
            WorkerId::from_str("complex&*-worker-name-0").unwrap(),
            WorkerId {
                name: "complex&*-worker-name-0".to_string()
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

        async fn task(job: u32, count: Data<Count>) {
            count.fetch_add(1, Ordering::Relaxed);
            if job == ITEMS - 1 {}
        }
        let worker = WorkerBuilder::new("rango-tango")
            .data(Count::default())
            .backend(in_memory);
        let worker = worker.build_fn(task);
        worker.run().await;
    }
}
