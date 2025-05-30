use std::{
    marker::PhantomData,
    sync::{Arc, RwLock},
    time::Duration,
};

use futures::Stream;
use tower::{
    layer::util::{Identity, Stack},
    Layer, Service, ServiceBuilder,
};

use crate::{
    backend::Backend, error::BoxDynError, layers::extensions::Data, request::Request, service_fn::{service_fn, ServiceFn}, shutdown::Shutdown, task::{attempt::Attempt, task_id::TaskId}, worker::{Event, EventHandler, Worker, WorkerContext, WorkerId}
};

/// Allows building a [`Worker`].
/// Usually the output is [`Worker<Ready>`]
pub struct WorkerBuilder<Req, Source, Middleware> {
    pub(crate) id: WorkerId,
    pub(crate) request: PhantomData<Req>,
    pub(crate) layer: ServiceBuilder<Middleware>,
    pub(crate) source: Source,
    pub(crate) event_handler: EventHandler,
    pub(crate) shutdown: Option<Shutdown>,
}

impl<Req, Source, Middleware> std::fmt::Debug for WorkerBuilder<Req, Source, Middleware> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerBuilder")
            .field("id", &self.id)
            .field("job", &std::any::type_name::<Req>())
            .field("layer", &std::any::type_name::<Middleware>())
            .field("source", &std::any::type_name::<Source>())
            .finish()
    }
}

impl WorkerBuilder<(), (), Identity> {
    /// Build a new [`WorkerBuilder`] instance with a name for the worker to build
    pub fn new<T: AsRef<str>>(name: T) -> WorkerBuilder<(), (), Identity> {
        WorkerBuilder {
            request: PhantomData,
            layer: ServiceBuilder::new(),
            source: (),
            id: WorkerId::new(name),
            event_handler: EventHandler::default(),
            shutdown: None,
        }
    }
}

impl WorkerBuilder<(), (), Identity> {
    /// Consume a stream directly
    #[deprecated(since = "0.6.0", note = "Consider using the `.backend`")]
    pub fn stream<
        NS: Stream<Item = Result<Option<Request<NJ, Ctx>>, BoxDynError>> + Send + 'static,
        NJ,
        Ctx,
    >(
        self,
        stream: NS,
    ) -> WorkerBuilder<Request<NJ, Ctx>, NS, Identity> {
        WorkerBuilder {
            request: PhantomData,
            layer: self.layer,
            source: stream,
            id: self.id,
            shutdown: self.shutdown,
            event_handler: self.event_handler,
        }
    }

    /// Set the source to a backend that implements [Backend]
    pub fn backend<NB: Backend<Request<NJ, Ctx>>, NJ, Ctx>(
        self,
        backend: NB,
    ) -> WorkerBuilder<Request<NJ, Ctx>, NB, Identity> {
        WorkerBuilder {
            request: PhantomData,
            layer: self.layer,
            source: backend,
            id: self.id,
            shutdown: self.shutdown,
            event_handler: self.event_handler,
        }
    }
}

impl<Req, M, B> WorkerBuilder<Req, B, M> {
    /// Allows of decorating the service that consumes jobs.
    /// Allows adding multiple [`tower`] middleware
    pub fn chain<NewLayer>(
        self,
        f: impl FnOnce(ServiceBuilder<M>) -> ServiceBuilder<NewLayer>,
    ) -> WorkerBuilder<Req, B, NewLayer> {
        let middleware = f(self.layer);

        WorkerBuilder {
            request: self.request,
            layer: middleware,
            id: self.id,
            source: self.source,
            shutdown: self.shutdown,
            event_handler: self.event_handler,
        }
    }
    /// Allows adding a single layer [tower] middleware
    pub fn layer<U>(self, layer: U) -> WorkerBuilder<Req, B, Stack<U, M>>
    where
        M: Layer<U>,
    {
        WorkerBuilder {
            request: self.request,
            source: self.source,
            layer: self.layer.layer(layer),
            id: self.id,
            shutdown: self.shutdown,
            event_handler: self.event_handler,
        }
    }

    /// Adds data to the context
    /// This will be shared by all requests
    pub fn data<D>(self, data: D) -> WorkerBuilder<Req, B, Stack<Data<D>, M>>
    where
        M: Layer<Data<D>>,
    {
        WorkerBuilder {
            request: self.request,
            source: self.source,
            layer: self.layer.layer(Data::new(data)),
            id: self.id,
            shutdown: self.shutdown,
            event_handler: self.event_handler,
        }
    }
}

/// Helper trait for building new Workers from [`WorkerBuilder`]
pub trait WorkerFactory<Req, Svc> {
    type Worker;
    fn build(self, service: Svc) -> Self::Worker;
}

pub trait WorkerFactoryWith<Req, Svc, Builder> {
    type Worker;
    fn build_with(self, builder: Builder) -> Self::Worker;
}

pub trait BuildWith<Builder> {
    type Worker;
    fn builder_with(self, builder: Builder) -> Self::Worker;
}

impl<Args, P, M, S, Ctx, Builder> WorkerFactoryWith<Request<Args, Ctx>, S, Builder>
    for WorkerBuilder<Request<Args, Ctx>, P, M>
where
    S: Service<Request<Args, Ctx>>,
    M: Layer<S>,
    P: Backend<Request<Args, Ctx>>,
    Builder: BuildWith<Self, Worker = Worker<Args, Ctx, P, S, M>>,
{
    type Worker = Worker<Args, Ctx, P, S, M>;
    fn build_with(self, builder: Builder) -> Self::Worker {
        builder.builder_with(self)
    }
}

impl<Args, P, M, S, Ctx> WorkerFactory<Request<Args, Ctx>, S>
    for WorkerBuilder<Request<Args, Ctx>, P, M>
where
    S: Service<Request<Args, Ctx>>,
    M: Layer<S>,
    P: Backend<Request<Args, Ctx>>,
{
    type Worker = Worker<Args, Ctx, P, S, M>;
    fn build(self, service: S) -> Self::Worker {
        let mut worker = Worker::new(self.id, self.source, service, self.layer);
        worker.event_handler = self
            .event_handler
            .write()
            .map(|mut d| d.take())
            .unwrap()
            .unwrap_or(Box::new(|_, _| {}));
        worker.shutdown = self.shutdown;
        worker
    }
}

pub struct LongRunningLayer;

impl<S> Layer<S> for LongRunningLayer {
    type Service = LongRunningService<S>;

    fn layer(&self, service: S) -> Self::Service {
        LongRunningService { service }
    }
}

pub struct LongRunningService<S> {
    service: S,
}

impl<S, Request> Service<Request> for LongRunningService<S>
where
    S: Service<Request>,
    Request: std::fmt::Debug,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        println!("request = {:?}", request);
        self.service.call(request)
    }
}

#[derive(Debug, Default)]
pub struct LongRunningConfig {
    pub max_duration: Option<Duration>,
}
impl LongRunningConfig {
    pub fn new(max_duration: Duration) -> Self {
        Self {
            max_duration: Some(max_duration),
        }
    }
}

pub trait RecordAttempt<Req, Source, Middleware>: Sized {
    fn record_attempts(self) -> WorkerBuilder<Req, Source, Middleware>;
}

impl<Args, P, M, Ctx> RecordAttempt<Request<Args, Ctx>, P, M>
    for WorkerBuilder<Request<Args, Ctx>, P, M>
where
    P: Backend<Request<Args, Ctx>>,
{
    fn record_attempts(self) -> WorkerBuilder<Request<Args, Ctx>, P, M> {
        let this = self;
        WorkerBuilder {
            id: this.id,
            request: this.request,
            layer: this.layer,
            source: this.source,
            shutdown: this.shutdown,
            event_handler: this.event_handler,
        }
    }
}

pub trait EventListenerExt<Req, Source, Middleware>: Sized {
    fn on_event<F: Fn(&WorkerContext, &Event) + Send + Sync + 'static>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Source, Stack<EventListenerLayer, Middleware>>;
}

#[derive(Clone)]
pub struct EventListenerLayer;

impl<S> Layer<S> for EventListenerLayer {
    type Service = EventListenerService<S>;

    fn layer(&self, service: S) -> Self::Service {
        EventListenerService { service }
    }
}

pub type EventListener = Arc<Box<dyn Fn(WorkerContext, Event) + Send + Sync>>;

#[derive(Clone)]
pub struct EventListenerService<S> {
    service: S,
}

impl<S, Args, Ctx> Service<Request<Args, Ctx>> for EventListenerService<S>
where
    S: Service<Request<Args, Ctx>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Args, Ctx>) -> Self::Future {
        // TODO
        // let ctx: &WorkerContext = request.get_checked().unwrap();
        self.service.call(request)
    }
}

impl<Args, P, M, Ctx> EventListenerExt<Request<Args, Ctx>, P, M>
    for WorkerBuilder<Request<Args, Ctx>, P, M>
where
    P: Backend<Request<Args, Ctx>>,
    M: Layer<EventListenerLayer>,
{
    fn on_event<F: Fn(&WorkerContext, &Event) + Send + Sync + 'static>(
        self,
        f: F,
    ) -> WorkerBuilder<Request<Args, Ctx>, P, Stack<EventListenerLayer, M>> {
        let new_fn = self
            .event_handler
            .write()
            .map(|mut res| {
                let current = res.take();
                match current {
                    Some(c) => {
                        let new: Box<dyn Fn(&WorkerContext, &Event) + Send + Sync + 'static> =
                            Box::new(move |ctx, ev| {
                                c(&ctx, &ev);
                                f(&ctx, &ev);
                            });
                        new
                    }
                    None => {
                        let new: Box<dyn Fn(&WorkerContext, &Event) + Send + Sync + 'static> =
                            Box::new(move |ctx, ev| {
                                f(&ctx, &ev);
                            });
                        new
                    }
                }
            })
            .unwrap();
        let _ = self.event_handler.write().map(|mut res| {
            res.insert(new_fn);
        });
        let this = self.layer(EventListenerLayer);
        WorkerBuilder {
            id: this.id,
            request: this.request,
            layer: this.layer,
            source: this.source,
            shutdown: this.shutdown,
            event_handler: this.event_handler,
        }
    }
}

/// Helper trait for building new Workers from [`WorkerBuilder`]
pub trait LongRunningExt<Req, Source, Middleware>: Sized {
    fn long_running(self) -> WorkerBuilder<Req, Source, Stack<LongRunningLayer, Middleware>> {
        self.long_running_with_cfg(Default::default())
    }
    fn long_running_with_cfg(
        self,
        cfg: LongRunningConfig,
    ) -> WorkerBuilder<Req, Source, Stack<LongRunningLayer, Middleware>>;
}

impl<Args, P, M, Ctx> LongRunningExt<Request<Args, Ctx>, P, M>
    for WorkerBuilder<Request<Args, Ctx>, P, M>
where
    M: Layer<LongRunningLayer>,
{
    fn long_running_with_cfg(
        self,
        cfg: LongRunningConfig,
    ) -> WorkerBuilder<Request<Args, Ctx>, P, Stack<LongRunningLayer, M>> {
        let this = self.layer(LongRunningLayer);
        WorkerBuilder {
            id: this.id,
            request: this.request,
            layer: this.layer,
            source: this.source,
            shutdown: this.shutdown,
            event_handler: this.event_handler,
        }
    }
}

pub struct LongRunningContext {
    pub task_set: Vec<TaskId>,
}

/// Helper trait for building new Workers from [`WorkerBuilder`]
pub trait WorkerFactoryFn<Req, F, FnArgs> {
    type Worker;
    /// Builds a [`WorkerFactoryFn`] using [`ServiceFn`]
    /// that can be used to generate new [`Worker`] using the `build_fn` method
    /// # Arguments
    ///
    /// * `f` - A functional service.
    ///
    /// # Examples
    ///
    /// A function can take many forms to allow flexibility
    /// - An async function with a single argument of the item being processed
    /// - An async function with an argument of the item being processed plus up-to 16 arguments that are extracted from the request [`Data`]
    ///
    /// A function can return:
    /// - ()
    /// - primitive
    /// - Result<T, E: Error>
    /// - impl IntoResponse
    ///
    /// ```rust
    /// # use apalis_core::layers::extensions::Data;
    /// #[derive(Debug)]
    /// struct Email;
    /// #[derive(Debug)]
    /// struct PgPool;
    ///
    /// async fn send_email(email: Email, data: Data<PgPool>) {
    ///     // Implementation of the task function?
    /// }
    /// ```
    ///
    fn build_fn(self, f: F) -> Self::Worker;
}

impl<Args, W, F, Ctx, FnArgs> WorkerFactoryFn<Request<Args, Ctx>, F, FnArgs> for W
where
    W: WorkerFactory<Request<Args, Ctx>, ServiceFn<F, Args, Ctx, FnArgs>>,
{
    type Worker = W::Worker;
    fn build_fn(self, f: F) -> W::Worker {
        self.build(service_fn(f))
    }
}
