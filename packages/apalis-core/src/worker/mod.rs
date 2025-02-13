use crate::backend::Backend;
use crate::error::{BoxDynError, Error};
use crate::layers::extensions::Data;
use crate::monitor::shutdown::Shutdown;
use crate::request::Request;
use crate::service_fn::FromRequest;
use crate::task::task_id::TaskId;
use call_all::CallAllUnordered;
use futures::future::{join, select, BoxFuture};
use futures::stream::BoxStream;
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
use tower::{Layer, Service, ServiceBuilder};

mod call_all;

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
    Engage(TaskId),
    /// Worker is idle, stream has no new request for now
    Idle,
    /// A custom event
    Custom(String),
    /// Worker encountered an error
    Error(BoxDynError),
    /// Worker stopped
    Stop,
    /// Worker completed all pending tasks
    Exit,
}

impl fmt::Display for Worker<Event> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let event_description = match &self.state {
            Event::Start => "Worker started".to_string(),
            Event::Engage(task_id) => format!("Worker engaged with Task ID: {}", task_id),
            Event::Idle => "Worker is idle".to_string(),
            Event::Custom(msg) => format!("Custom event: {}", msg),
            Event::Error(err) => format!("Worker encountered an error: {}", err),
            Event::Stop => "Worker stopped".to_string(),
            Event::Exit => "Worker completed all pending tasks and exited".to_string(),
        };

        write!(f, "Worker [{}]: {}", self.id.name, event_description)
    }
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
#[derive(Debug, Clone, Serialize)]
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

impl Worker<Context> {
    /// Allows workers to emit events
    pub fn emit(&self, event: Event) -> bool {
        if let Some(handler) = self.state.event_handler.read().unwrap().as_ref() {
            handler(Worker {
                id: self.id().clone(),
                state: event,
            });
            return true;
        }
        false
    }
    /// Start running the worker
    pub fn start(&self) {
        self.state.running.store(true, Ordering::Relaxed);
        self.state.is_ready.store(true, Ordering::Release);
        self.emit(Event::Start);
    }
}

impl<Req, Ctx> FromRequest<Request<Req, Ctx>> for Worker<Context> {
    fn from_request(req: &Request<Req, Ctx>) -> Result<Self, Error> {
        req.parts.data.get_checked().cloned()
    }
}

impl<S, P> Worker<Ready<S, P>> {
    /// Add an event handler to the worker
    pub fn on_event<F: Fn(Worker<Event>) + Send + Sync + 'static>(self, f: F) -> Self {
        let _ = self.event_handler.write().map(|mut res| {
            let _ = res.insert(Box::new(f));
        });
        self
    }

    fn poll_jobs<Svc, Stm, Req, Ctx>(
        worker: Worker<Context>,
        service: Svc,
        stream: Stm,
    ) -> BoxStream<'static, ()>
    where
        Svc: Service<Request<Req, Ctx>> + Send + 'static,
        Stm: Stream<Item = Result<Option<Request<Req, Ctx>>, Error>> + Send + Unpin + 'static,
        Req: Send + 'static,
        Svc::Future: Send,
        Svc::Error: Send + 'static + Into<BoxDynError>,
        Ctx: Send + 'static,
    {
        let w = worker.clone();
        let stream = stream.filter_map(move |result| {
            let worker = worker.clone();

            async move {
                match result {
                    Ok(Some(request)) => {
                        worker.emit(Event::Engage(request.parts.task_id.clone()));
                        Some(request)
                    }
                    Ok(None) => {
                        worker.emit(Event::Idle);
                        None
                    }
                    Err(err) => {
                        worker.emit(Event::Error(Box::new(err)));
                        None
                    }
                }
            }
        });
        let stream = CallAllUnordered::new(service, stream).map(move |res| {
            if let Err(error) = res {
                let error = error.into();
                if let Some(Error::MissingData(_)) = error.downcast_ref::<Error>() {
                    w.stop();
                }
                w.emit(Event::Error(error));
            }
        });
        stream.boxed()
    }
    /// Start a worker
    pub fn run<Req, Ctx>(self) -> Runnable
    where
        S: Service<Request<Req, Ctx>> + 'static,
        P: Backend<Request<Req, Ctx>> + 'static,
        Req: Send + 'static,
        S::Error: Send + 'static + Into<BoxDynError>,
        P::Stream: Unpin + Send + 'static,
        P::Layer: Layer<S>,
        <P::Layer as Layer<S>>::Service: Service<Request<Req, Ctx>> + Send,
        <<P::Layer as Layer<S>>::Service as Service<Request<Req, Ctx>>>::Future: Send,
        <<P::Layer as Layer<S>>::Service as Service<Request<Req, Ctx>>>::Error:
            Send + Into<BoxDynError>,
        Ctx: Send + 'static,
    {
        fn type_name_of_val<T>(_t: &T) -> &'static str {
            std::any::type_name::<T>()
        }
        let service = self.state.service;
        let worker_id = self.id;
        let ctx = Context {
            running: Arc::default(),
            task_count: Arc::default(),
            waker: Arc::default(),
            shutdown: self.state.shutdown,
            event_handler: self.state.event_handler.clone(),
            is_ready: Arc::default(),
            service: type_name_of_val(&service).to_owned(),
        };
        let worker = Worker {
            id: worker_id.clone(),
            state: ctx.clone(),
        };
        let backend = self.state.backend;

        let poller = backend.poll(&worker);
        let stream = poller.stream;
        let heartbeat = poller.heartbeat.boxed();
        let layer = poller.layer;
        let service = ServiceBuilder::new()
            .layer(TrackerLayer::new(worker.state.clone()))
            .layer(ReadinessLayer::new(worker.state.is_ready.clone()))
            .layer(Data::new(worker.clone()))
            .layer(layer)
            .service(service);

        Runnable {
            poller: Self::poll_jobs(worker.clone(), service, stream),
            heartbeat,
            worker,
            running: false,
        }
    }
}

/// A `Runnable` represents a unit of work that manages a worker's lifecycle and execution flow.
///
/// The `Runnable` struct is responsible for coordinating the core tasks of a worker, such as polling for jobs,
/// maintaining heartbeats, and tracking its running state. It integrates various components required for
/// the worker to operate effectively within an asynchronous runtime.
#[must_use = "A Runnable must be awaited of no jobs will be consumed"]
pub struct Runnable {
    poller: BoxStream<'static, ()>,
    heartbeat: BoxFuture<'static, ()>,
    worker: Worker<Context>,
    running: bool,
}

impl Runnable {
    /// Returns a handle to the worker, allowing control and functionality like stopping
    pub fn get_handle(&self) -> Worker<Context> {
        self.worker.clone()
    }
}

impl fmt::Debug for Runnable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runnable")
            .field("poller", &"<stream>")
            .field("heartbeat", &"<future>")
            .field("worker", &self.worker)
            .field("running", &self.running)
            .finish()
    }
}

impl Future for Runnable {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let poller = &mut this.poller;
        let heartbeat = &mut this.heartbeat;
        let worker = &mut this.worker;

        let poller_future = async { while (poller.next().await).is_some() {} };

        if !this.running {
            worker.start();
            this.running = true;
        }
        let combined = Box::pin(join(poller_future, heartbeat.as_mut()));

        let mut combined = select(
            combined,
            worker.state.clone().map(|_| worker.emit(Event::Stop)),
        )
        .boxed();
        match Pin::new(&mut combined).poll(cx) {
            Poll::Ready(_) => {
                worker.emit(Event::Exit);
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Stores the Workers context
#[derive(Clone, Default)]
pub struct Context {
    task_count: Arc<AtomicUsize>,
    waker: Arc<Mutex<Option<Waker>>>,
    running: Arc<AtomicBool>,
    shutdown: Option<Shutdown>,
    event_handler: EventHandler,
    is_ready: Arc<AtomicBool>,
    service: String,
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerContext")
            .field("shutdown", &["Shutdown handle"])
            .field("task_count", &self.task_count)
            .field("running", &self.running)
            .field("service", &self.service)
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
    pub fn track<F: Future>(&self, task: F) -> Tracked<F> {
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
    }

    fn end_task(&self) {
        if self.task_count.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.wake();
        }
    }

    pub(crate) fn wake(&self) {
        if let Ok(waker) = self.waker.lock() {
            if let Some(waker) = &*waker {
                waker.wake_by_ref();
            }
        }
    }

    /// Returns whether the worker is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Returns the current futures in the worker domain
    /// This include futures spawned via `worker.track`
    pub fn task_count(&self) -> usize {
        self.task_count.load(Ordering::Relaxed)
    }

    /// Returns whether the worker has pending tasks
    pub fn has_pending_tasks(&self) -> bool {
        self.task_count.load(Ordering::Relaxed) > 0
    }

    /// Is the shutdown token called
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown
            .as_ref()
            .map(|s| !self.is_running() || s.is_shutting_down())
            .unwrap_or(!self.is_running())
    }

    fn add_waker(&self, cx: &mut TaskCtx<'_>) {
        if let Ok(mut waker_guard) = self.waker.lock() {
            if waker_guard
                .as_ref()
                .map_or(true, |stored_waker| !stored_waker.will_wake(cx.waker()))
            {
                *waker_guard = Some(cx.waker().clone());
            }
        }
    }

    /// Checks if the stored waker matches the current one.
    fn has_recent_waker(&self, cx: &TaskCtx<'_>) -> bool {
        if let Ok(waker_guard) = self.waker.lock() {
            if let Some(stored_waker) = &*waker_guard {
                return stored_waker.will_wake(cx.waker());
            }
        }
        false
    }

    /// Returns if the worker is ready to consume new tasks
    pub fn is_ready(&self) -> bool {
        self.is_ready.load(Ordering::Acquire) && !self.is_shutting_down()
    }

    /// Get the type of service
    pub fn get_service(&self) -> &String {
        &self.service
    }
}

impl Future for Context {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut TaskCtx<'_>) -> Poll<()> {
        let task_count = self.task_count.load(Ordering::Relaxed);
        if self.is_shutting_down() && task_count == 0 {
            Poll::Ready(())
        } else {
            if !self.has_recent_waker(cx) {
                self.add_waker(cx);
            }
            Poll::Pending
        }
    }
}

#[derive(Debug, Clone)]
struct TrackerLayer {
    ctx: Context,
}

impl TrackerLayer {
    fn new(ctx: Context) -> Self {
        Self { ctx }
    }
}

impl<S> Layer<S> for TrackerLayer {
    type Service = TrackerService<S>;

    fn layer(&self, service: S) -> Self::Service {
        TrackerService {
            ctx: self.ctx.clone(),
            service,
        }
    }
}
#[derive(Debug, Clone)]
struct TrackerService<S> {
    ctx: Context,
    service: S,
}

impl<S, Req, Ctx> Service<Request<Req, Ctx>> for TrackerService<S>
where
    S: Service<Request<Req, Ctx>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Tracked<S::Future>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Req, Ctx>) -> Self::Future {
        request.parts.attempt.increment();
        self.ctx.track(self.service.call(request))
    }
}

#[derive(Clone)]
struct ReadinessLayer {
    is_ready: Arc<AtomicBool>,
}

impl ReadinessLayer {
    fn new(is_ready: Arc<AtomicBool>) -> Self {
        Self { is_ready }
    }
}

impl<S> Layer<S> for ReadinessLayer {
    type Service = ReadinessService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ReadinessService {
            inner,
            is_ready: self.is_ready.clone(),
        }
    }
}

struct ReadinessService<S> {
    inner: S,
    is_ready: Arc<AtomicBool>,
}

impl<S, Request> Service<Request> for ReadinessService<S>
where
    S: Service<Request>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Delegate poll_ready to the inner service
        let result = self.inner.poll_ready(cx);
        // Update the readiness state based on the result
        match &result {
            Poll::Ready(Ok(_)) => self.is_ready.store(true, Ordering::Release),
            Poll::Pending | Poll::Ready(Err(_)) => self.is_ready.store(false, Ordering::Release),
        }

        result
    }

    fn call(&mut self, req: Request) -> Self::Future {
        self.inner.call(req)
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

        async fn task(job: u32, count: Data<Count>, worker: Worker<Context>) {
            count.fetch_add(1, Ordering::Relaxed);
            if job == ITEMS - 1 {
                worker.stop();
            }
        }
        let worker = WorkerBuilder::new("rango-tango")
            .data(Count::default())
            .backend(in_memory);
        let worker = worker.build_fn(task);
        worker.run().await;
    }
}
