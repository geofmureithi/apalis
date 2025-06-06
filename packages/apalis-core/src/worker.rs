use crate::backend::Backend;
use crate::data::{Data, MissingDataError};
use crate::error::BoxDynError;
use crate::request::Request;
use crate::service_fn::FromRequest;
use crate::shutdown::Shutdown;
use crate::task::task_id::TaskId;
use call_all::CallAllUnordered;
use futures::channel::mpsc;
use futures::future::{join, select, BoxFuture};
use futures::stream::BoxStream;
use futures::{Future, FutureExt, SinkExt, Stream, StreamExt};
use pin_project_lite::pin_project;
use serde::{Deserialize, Serialize};
use std::any::type_name;
use std::fmt::Debug;
use std::fmt::{self, Display};
use std::marker::PhantomData;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context as TaskCtx, Poll, Waker};
use thiserror::Error;
use tower::{Layer, Service, ServiceBuilder};

/// A worker name wrapper usually used by Worker builder
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct WorkerId {
    name: String,
}

impl Default for WorkerId {
    fn default() -> Self {
        WorkerId::new("default-worker")
    }
}

/// An event handler for [`Worker`]
pub type EventHandler = Arc<RwLock<Option<Box<dyn Fn(&WorkerContext, &Event) + Send + Sync>>>>;

pub type CtxEventHandler = Arc<Box<dyn Fn(&WorkerContext, &Event) + Send + Sync>>;

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

    /// Worker did a heartbeat
    HeartBeat,
    /// A custom event
    Custom(String),
    /// A result of processing
    Success,
    /// Worker encountered an error
    Error(BoxDynError),
    /// Worker stopped
    Stop,
    /// Worker completed all pending tasks
    Exit,
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let event_description = match &self {
            Event::Start => "Worker started".to_string(),
            Event::Engage(task_id) => format!("Worker engaged with Task ID: {}", task_id),
            Event::Idle => "Worker is idle".to_string(),
            Event::Custom(msg) => format!("Custom event: {}", msg),
            Event::Error(err) => format!("Worker encountered an error: {}", err),
            Event::Stop => "Worker stopped".to_string(),
            Event::Exit => "Worker completed all pending tasks and exited".to_string(),
            Event::HeartBeat => "Worker Heartbeat".to_owned(),
            Event::Success => "Worker completed task successfully".to_string(),
        };

        write!(f, "WorkerEvent: {}", event_description)
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
pub struct Worker<Args, Ctx, Backend, Svc, Middleware> {
    pub(crate) id: WorkerId,
    pub(crate) backend: Backend,
    pub(crate) service: Svc,
    pub(crate) middleware: ServiceBuilder<Middleware>,
    pub(crate) req: PhantomData<Request<Args, Ctx>>,
    pub(crate) shutdown: Option<Shutdown>,
    pub(crate) event_handler: Box<dyn Fn(&WorkerContext, &Event) + Send + Sync>,
}

impl<Args, Ctx, B, Svc, Middleware> fmt::Debug for Worker<Args, Ctx, B, Svc, Middleware>
where
    Svc: fmt::Debug,
    B: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BasicWorker")
            .field("service", &self.service)
            .field("backend", &self.backend)
            .finish()
    }
}

impl<Args, Ctx, B, Svc, M> Worker<Args, Ctx, B, Svc, M> {
    /// Build a worker that is ready for execution
    pub fn new(id: WorkerId, backend: B, service: Svc, layers: ServiceBuilder<M>) -> Self {
        Worker {
            id,
            backend,
            service,
            middleware: layers,
            req: PhantomData,
            shutdown: None,
            event_handler: Box::new(|_, _| {}),
        }
    }
}

impl WorkerContext {
    /// Allows workers to emit events
    pub fn emit(&self, event: Event) -> bool {
        let handler = self.event_handler.as_ref();
        handler(self, &event);
        return true;
    }
    /// Start running the worker
    pub fn start(&self) {
        self.running.store(true, Ordering::Relaxed);
        self.is_ready.store(true, Ordering::Release);
        self.emit(Event::Start);
    }

    pub(crate) fn wrap_listener<F: Fn(&WorkerContext, &Event) + Send + Sync + 'static>(
        &mut self,
        f: F,
    ) {
        let cur = self.event_handler.clone();
        let new: Box<dyn Fn(&WorkerContext, &Event) + Send + Sync + 'static> =
            Box::new(move |ctx, ev| {
                f(&ctx, &ev);
                cur(&ctx, &ev);
            });
        self.event_handler = Arc::new(new);
    }

    pub fn new<S>(id: &WorkerId) -> Self {
        Self {
            id: Arc::new(id.clone()),
            service: type_name::<S>(),
            task_count: Default::default(),
            waker: Default::default(),
            running: Default::default(),
            shutdown: Default::default(),
            event_handler: Arc::new(Box::new(|_, _| {
                // noop
            })),
            is_ready: Default::default(),
        }
    }
}

impl<Req: Sync, Ctx: Sync> FromRequest<Request<Req, Ctx>> for WorkerContext {
    type Error = MissingDataError;
    async fn from_request(req: &Request<Req, Ctx>) -> Result<Self, Self::Error> {
        req.parts.data.get_checked().cloned()
    }
}

impl<Args, Ctx, S, B, M> Worker<Args, Ctx, B, S, M>
where
    B: Backend<Request<Args, Ctx>>,
    S: Service<Request<Args, Ctx>> + Send + 'static,
    B::Stream: Unpin + Send + 'static,
    B::Beat: Unpin + Send + 'static,
    Args: Send + 'static,
    Ctx: Send + 'static,
    B::Error: Into<BoxDynError> + Send + 'static,
    // S::Future: Send,
    // S::Error: std::error::Error + Send + Sync + 'static,
    // TrackerLayer, Stack<ReadinessLayer, Stack<ServiceBuilder<M>, Stack<Data<WorkerContext>, Identity>>>
    M: Layer<ReadinessService<TrackerService<S>>>,
    // M: Layer<AddExtension<ReadinessService<TrackerService<S>>, WorkerContext>>,
    B::Layer: Layer<M::Service>,
    <B::Layer as Layer<M::Service>>::Service: Service<Request<Args, Ctx>> + Send + 'static,
    <<B::Layer as Layer<M::Service>>::Service as Service<Request<Args, Ctx>>>::Error:
        Into<BoxDynError> + Send + Sync + 'static,
    <<B::Layer as Layer<M::Service>>::Service as Service<Request<Args, Ctx>>>::Future: Send,
    M::Service: Service<Request<Args, Ctx>> + Send + 'static,
    <<M as Layer<ReadinessService<TrackerService<S>>>>::Service as Service<Request<Args, Ctx>>>::Future: Send,
    <<M as Layer<ReadinessService<TrackerService<S>>>>::Service as Service<Request<Args, Ctx>>>::Error: Into<BoxDynError> + Send + Sync +'static
{
    pub fn run(self) -> BoxFuture<'static, Result<(), ()>> {
        let mut ctx = WorkerContext::new::<M::Service>(&self.id);
        self.run_with_ctx(&mut ctx)
    }

    pub fn run_with_ctx(self, ctx: &mut WorkerContext) -> BoxFuture<'static, Result<(), ()>> {
        let backend = self.backend;
        let event_handler = self.event_handler;
        ctx.wrap_listener(event_handler);
        let worker = ctx.clone();
        let service = ServiceBuilder::new()
            .layer(Data::new(worker.clone()))
            .layer(self.middleware.into_inner())
            .layer(ReadinessLayer::new(worker.is_ready.clone()))
            .layer(TrackerLayer::new(worker.clone()))
            .service(self.service);
        let mut heartbeat = backend.heartbeat();
        let heartbeat = async move {
            while let Some(_) = heartbeat.next().await {

            }
        }
        .boxed();

        let mut stream = backend.poll(&worker);

        let mut jobs = poll_jobs(service, stream);
        let w = worker.clone();
        let poller_future = async move {
            while let Some(event) = jobs.next().await {
                w.emit(event);
            }
        };
        let combined = Box::pin(join(poller_future, heartbeat));
        let w = worker.clone();
        let mut combined =
            select(combined, worker.clone()).boxed();

        let fut = async move {
            worker.start();
            combined.await;
            worker.emit(Event::Exit);
            Ok(())
        };

        fut.boxed()
    }

    pub fn stream(self) -> impl Stream<Item = Event>{
        let mut ctx = WorkerContext::new::<M::Service>(&self.id);
        self.stream_with_ctx(&mut ctx)
    }

    pub fn stream_with_ctx(self, ctx: &mut WorkerContext) -> impl Stream<Item = Event>{
         let backend = self.backend;
        let event_handler = self.event_handler;
        ctx.wrap_listener(event_handler);
        let worker = ctx.clone();
        let service = ServiceBuilder::new()
            .layer(Data::new(worker.clone()))
            .layer(self.middleware.into_inner())
            .layer(ReadinessLayer::new(worker.is_ready.clone()))
            .layer(TrackerLayer::new(worker.clone()))
            .service(self.service);
        let heartbeat = backend.heartbeat().map(|_| Event::HeartBeat);

        let stream = backend.poll(&worker);

        let jobs = poll_jobs(service, stream);
        let work_stream = futures::stream_select!(heartbeat, jobs);
        work_stream

    }
}

fn poll_jobs<Svc, Stm, Req, Ctx, E: Into<BoxDynError> + Send + 'static>(
    service: Svc,
    stream: Stm,
) -> BoxStream<'static, Event>
where
    Svc: Service<Request<Req, Ctx>> + Send + 'static,
    Stm: Stream<Item = Result<Option<Request<Req, Ctx>>, E>> + Send + Unpin + 'static,
    Req: Send + 'static,
    Svc::Future: Send,
    Ctx: Send + 'static,
    Svc::Error: Into<BoxDynError> + Sync + Send,
{
    let (tx, rx) = mpsc::channel(10);
    let txx = tx.clone();
    let stream = stream.filter_map(move |result| {
        let mut txx = txx.clone();

        async move {
            match result {
                Ok(Some(request)) => {
                    txx.send(Event::Engage(request.parts.task_id.clone()))
                        .await
                        .unwrap();
                    Some(request)
                }
                Ok(None) => {
                    txx.send(Event::Idle).await.unwrap();
                    None
                }
                Err(err) => {
                    txx.send(Event::Error(err.into())).await.unwrap();
                    None
                }
            }
        }
    });
    let stream = CallAllUnordered::new(service, stream).map(|r| match r {
        Ok(_) => Event::Success,
        Err(err) => Event::Error(err.into()),
    });
    let stream = futures::stream::select(rx, stream);
    stream.boxed()
}

/// Stores the Workers context
#[derive(Clone)]
pub struct WorkerContext {
    pub(crate) id: Arc<WorkerId>,
    task_count: Arc<AtomicUsize>,
    waker: Arc<Mutex<Option<Waker>>>,
    running: Arc<AtomicBool>,
    shutdown: Option<Shutdown>,
    event_handler: CtxEventHandler,
    is_ready: Arc<AtomicBool>,
    pub(crate) service: &'static str,
}

impl fmt::Debug for WorkerContext {
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
        ctx: WorkerContext,
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

impl WorkerContext {
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
        self.wake();
        self.emit(Event::Stop);
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
    pub fn get_service(&self) -> &str {
        &self.service
    }
}

impl Future for WorkerContext {
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
    ctx: WorkerContext,
}

impl TrackerLayer {
    fn new(ctx: WorkerContext) -> Self {
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
pub struct TrackerService<S> {
    ctx: WorkerContext,
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

pub struct ReadinessService<S> {
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

mod call_all {
    use futures::{ready, stream::FuturesUnordered, Stream};
    use pin_project_lite::pin_project;
    use std::{
        fmt,
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };
    use tower::Service;

    pin_project! {
        /// A stream of responses received from the inner service in received order.
        #[derive(Debug)]
        pub(super) struct CallAllUnordered<Svc, S>
        where
            Svc: Service<S::Item>,
            S: Stream,
        {
            #[pin]
            inner: CallAll<Svc, S, FuturesUnordered<Svc::Future>>,
        }
    }

    impl<Svc, S> CallAllUnordered<Svc, S>
    where
        Svc: Service<S::Item>,
        S: Stream,
    {
        /// Create new [`CallAllUnordered`] combinator.
        ///
        /// [`Stream`]: https://docs.rs/futures/latest/futures/stream/trait.Stream.html
        pub(super) fn new(service: Svc, stream: S) -> CallAllUnordered<Svc, S> {
            CallAllUnordered {
                inner: CallAll::new(service, stream, FuturesUnordered::new()),
            }
        }
    }

    impl<Svc, S> Stream for CallAllUnordered<Svc, S>
    where
        Svc: Service<S::Item>,
        S: Stream,
    {
        type Item = Result<Svc::Response, Svc::Error>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.project().inner.poll_next(cx)
        }
    }

    impl<F: Future> Drive<F> for FuturesUnordered<F> {
        fn is_empty(&self) -> bool {
            FuturesUnordered::is_empty(self)
        }

        fn push(&mut self, future: F) {
            FuturesUnordered::push(self, future)
        }

        fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Option<F::Output>> {
            Stream::poll_next(Pin::new(self), cx)
        }
    }

    pin_project! {
        /// The [`Future`] returned by the [`ServiceExt::call_all`] combinator.
        pub(crate) struct CallAll<Svc, S, Q>
        where
            S: Stream,
        {
            service: Option<Svc>,
            #[pin]
            stream: S,
            queue: Q,
            eof: bool,
            curr_req: Option<S::Item>
        }
    }

    impl<Svc, S, Q> fmt::Debug for CallAll<Svc, S, Q>
    where
        Svc: fmt::Debug,
        S: Stream + fmt::Debug,
    {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("CallAll")
                .field("service", &self.service)
                .field("stream", &self.stream)
                .field("eof", &self.eof)
                .finish()
        }
    }

    pub(crate) trait Drive<F: Future> {
        fn is_empty(&self) -> bool;

        fn push(&mut self, future: F);

        fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Option<F::Output>>;
    }

    impl<Svc, S, Q> CallAll<Svc, S, Q>
    where
        Svc: Service<S::Item>,
        S: Stream,
        Q: Drive<Svc::Future>,
    {
        pub(crate) const fn new(service: Svc, stream: S, queue: Q) -> CallAll<Svc, S, Q> {
            CallAll {
                service: Some(service),
                stream,
                queue,
                eof: false,
                curr_req: None,
            }
        }
    }

    impl<Svc, S, Q> Stream for CallAll<Svc, S, Q>
    where
        Svc: Service<S::Item>,
        S: Stream,
        Q: Drive<Svc::Future>,
    {
        type Item = Result<Svc::Response, Svc::Error>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let mut this = self.project();

            loop {
                // First, see if we have any responses to yield
                if let Poll::Ready(r) = this.queue.poll(cx) {
                    if let Some(rsp) = r.transpose()? {
                        return Poll::Ready(Some(Ok(rsp)));
                    }
                }

                // If there are no more requests coming, check if we're done
                if *this.eof {
                    if this.queue.is_empty() {
                        return Poll::Ready(None);
                    } else {
                        return Poll::Pending;
                    }
                }

                // Then, see that the service is ready for another request
                let svc = this
                    .service
                    .as_mut()
                    .expect("Using CallAll after extracting inner Service");

                if let Err(e) = ready!(svc.poll_ready(cx)) {
                    // Set eof to prevent the service from being called again after a `poll_ready` error
                    *this.eof = true;
                    return Poll::Ready(Some(Err(e)));
                }

                // If not done, and we don't have a stored request, gather the next request from the
                // stream (if there is one), or return `Pending` if the stream is not ready.
                if this.curr_req.is_none() {
                    *this.curr_req = match ready!(this.stream.as_mut().poll_next(cx)) {
                        Some(next_req) => Some(next_req),
                        None => {
                            // Mark that there will be no more requests.
                            *this.eof = true;
                            continue;
                        }
                    };
                }
                // Unwrap: The check above always sets `this.curr_req` if none.
                this.queue.push(svc.call(this.curr_req.take().unwrap()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Deref, sync::atomic::AtomicUsize, time::Duration};

    use crate::{
        backend::Push,
        builder::{WorkerBuilder, WorkerFactory, WorkerFactoryFn},
        ext::{
            ack::AcknowledgementExt, event_listener::EventListenerExt,
            long_running::LongRunningExt, record_attempt::RecordAttempt,
        },
        memory::MemoryStorage,
        service_fn::{self, service_fn, ServiceFn},
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
        let in_memory = MemoryStorage::default();
        let mut handle = in_memory.clone();

        tokio::spawn(async move {
            for i in 0..ITEMS {
                handle.push(i).await.unwrap();
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

        async fn task(job: u32, count: Data<Count>, ctx: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            count.fetch_add(1, Ordering::Relaxed);
            if job == ITEMS - 1 {
                ctx.stop();
                return Err("Worker stopped!")?;
            }
            Ok(())
        }

        struct MyAcknowledger;
        struct WebhookService;

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .data(Count::default())
            .record_attempts()
            .long_running()
            .ack_with(MyAcknowledger)
            .ack_with(|| WebhookService)
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {:?}", ctx, ev);
            })
            .build_fn(task);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn it_streams() {
        let in_memory = MemoryStorage::default();
        let mut handle = in_memory.clone();

        tokio::spawn(async move {
            for i in 0..ITEMS {
                handle.push(i).await.unwrap();
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

        async fn task(job: u32, count: Data<Count>, worker: WorkerContext) {
            tokio::time::sleep(Duration::from_secs(1)).await;
            count.fetch_add(1, Ordering::Relaxed);
            if job == ITEMS - 1 {
                worker.stop();
                tokio::time::sleep(Duration::from_secs(1)).await;
                println!("CTX {:?}", worker);
                // panic!("boo");
            }
        }
        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .data(Count::default())
            .record_attempts()
            .long_running()
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {:?}", ctx, ev);
            })
            .build_fn(task);
        let mut event_stream = worker.stream();
        while let Some(ev) = event_stream.next().await {
            println!("On Event = {:?}", ev);
            if let Event::Stop = ev {
                break;
            }
        }
    }
}
