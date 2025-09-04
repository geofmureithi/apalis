//! Builder types for composing and building workers.
//!
//! The `WorkerBuilder` component is the recommended
//! way to construct [`Worker`] instances in a flexible and
//! composable manner.
//!
//! The builder pattern enables customization of various parts of a worker,
//! including:
//!
//! - Setting a backend that implements the [`Backend`] trait
//! - Adding application state (shared [`Data`])
//! - Decorating the service pipeline using [`tower`] middleware
//! - Handling lifecycle events with `on_event`
//! - Providing task processing logic using either a [`Service`] or an async function via `build_fn`
//!
//! ## Basic usage
//!
//! ```rust,no_run
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_core::backend::memory::MemoryStorage;
//! # use apalis_core::worker::context::WorkerContext;
//! # use apalis_core::task::data::Data;
//! 
//! # #[tokio::main]
//! # async fn main() {
//! async fn task(job: u32, count: Data<usize>, ctx: WorkerContext) {
//!     println!("Received job: {job:?}");
//!     // Do something with count or ctx
//! }
//!
//! let worker = WorkerBuilder::new("rango-tango")
//!     .backend(in_memory)
//!     .data(0usize)
//!     .on_event(|ctx, ev| {
//!         println!("On Event = {:?}", ev);
//!     })
//!     .build_fn(task);
//!
//! worker.run().await.unwrap();
//! #}
//! ```
//! # Order
//!
//! The order in which layers are added impacts how requests are handled. Layers
//! that are added first will be called with the request first. The argument to
//! `service` will be last to see the request.
//!
//! ```
//! # // this (and other) doctest is ignored because we don't have a way
//! # // to say that it should only be run with cfg(feature = "...")
//! # use tower_service::Service;
//! # use tower::builder::ServiceBuilder;
//! # #[cfg(all(feature = "buffer", feature = "limit"))]
//! # async fn wrap<S>(svc: S) where S: Service<(), Error = &'static str> + 'static + Send, S::Future: Send {
//! WorkerBuilder::new()
//!     .buffer(100)
//!     .concurrency_limit(10)
//!     .build(svc)
//! # ;
//! # }
//! ```
//!
//! In the above example, the buffer layer receives the request first followed
//! by `concurrency_limit`. `buffer` enables up to 100 request to be in-flight
//! **on top of** the requests that have already been forwarded to the next
//! layer. Combined with `concurrency_limit`, this allows up to 110 requests to be
//! in-flight.
//!
//! ```
//! # use tower_service::Service;
//! # use tower::builder::ServiceBuilder;
//! # #[cfg(all(feature = "buffer", feature = "limit"))]
//! # async fn wrap<S>(svc: S) where S: Service<(), Error = &'static str> + 'static + Send, S::Future: Send {
//! WorkerBuilder::new()
//!     .concurrency_limit(10)
//!     .buffer(100)
//!     .build(svc)
//! # ;
//! # }
//! ```
//!
//! The above example is similar, but the order of layers is reversed. Now,
//! `concurrency_limit` applies first and only allows 10 requests to be in-flight
//! total.
//!
//! ## Features
//!
//! - [`WorkerBuilder`] starts empty (`new(name)`), and then can be extended using:
//!     - `.backend(...)`: Sets the task source.
//!     - `.data(...)`: Injects shared application data.
//!     - `.layer(...)`: Adds custom middleware.
//!     - `.build(service)`: Consumes the builder and a [`Service`] to construct the final [`Worker`].
use std::marker::PhantomData;

use tower_layer::{Identity, Layer, Stack};
use tower_service::Service;

use crate::{
    backend::Backend,
    monitor::shutdown::Shutdown,
    task::{data::Data, Task},
    worker::{event::EventHandlerBuilder, Worker},
};

/// Declaratively builds a [`Worker`]
pub struct WorkerBuilder<Args, Ctx, Source, Middleware> {
    pub(crate) name: String,
    pub(crate) request: PhantomData<(Args, Ctx)>,
    pub(crate) layer: Middleware,
    pub(crate) source: Source,
    pub(crate) event_handler: EventHandlerBuilder,
    pub(crate) shutdown: Option<Shutdown>,
}

impl<Args, Ctx, Source, Middleware> std::fmt::Debug
    for WorkerBuilder<Args, Ctx, Source, Middleware>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerBuilder")
            .field("id", &self.name)
            .field("job", &std::any::type_name::<(Args, Ctx)>())
            .field("layer", &std::any::type_name::<Middleware>())
            .field("source", &std::any::type_name::<Source>())
            .finish()
    }
}

impl WorkerBuilder<(), (), (), Identity> {
    /// Build a new [`WorkerBuilder`] instance with a name for the worker to build
    pub fn new<T: AsRef<str>>(name: T) -> WorkerBuilder<(), (), (), Identity> {
        WorkerBuilder {
            request: PhantomData,
            layer: Identity::new(),
            source: (),
            name: name.as_ref().to_owned(),
            event_handler: EventHandlerBuilder::default(),
            shutdown: None,
        }
    }
}

impl WorkerBuilder<(), (), (), Identity> {
    /// Set the source to a backend that implements [Backend]
    pub fn backend<NB: Backend<NJ>, NJ, Ctx>(
        self,
        backend: NB,
    ) -> WorkerBuilder<NJ, Ctx, NB, Identity> {
        WorkerBuilder {
            request: PhantomData,
            layer: self.layer,
            source: backend,
            name: self.name,
            shutdown: self.shutdown,
            event_handler: self.event_handler,
        }
    }
}

impl<Args, Ctx, M, B> WorkerBuilder<Args, Ctx, B, M>
where
    B: Backend<Args>,
{
    /// Allows of decorating the service that consumes jobs.
    /// Allows adding multiple middleware in one call
    pub fn chain<NewLayer>(
        self,
        f: impl FnOnce(M) -> NewLayer,
    ) -> WorkerBuilder<Args, Ctx, B, NewLayer> {
        let middleware = f(self.layer);

        WorkerBuilder {
            request: self.request,
            layer: middleware,
            name: self.name,
            source: self.source,
            shutdown: self.shutdown,
            event_handler: self.event_handler,
        }
    }
    /// Allows adding a single layer [tower] middleware
    pub fn layer<U>(self, layer: U) -> WorkerBuilder<Args, Ctx, B, Stack<U, M>>
    // where
    //     M: Layer<U>,
    {
        WorkerBuilder {
            request: self.request,
            source: self.source,
            layer: Stack::new(layer, self.layer),
            name: self.name,
            shutdown: self.shutdown,
            event_handler: self.event_handler,
        }
    }

    /// Adds data to the context
    /// This will be shared by all requests
    pub fn data<D>(self, data: D) -> WorkerBuilder<Args, Ctx, B, Stack<Data<D>, M>>
    where
        M: Layer<Data<D>>,
    {
        WorkerBuilder {
            request: self.request,
            source: self.source,
            layer: Stack::new(Data::new(data), self.layer),
            name: self.name,
            shutdown: self.shutdown,
            event_handler: self.event_handler,
        }
    }

    #[inline]
    /// A helper for checking that the builder can build a worker with the provided service
    pub fn build_check<W, Svc>(self, service: Svc)
    where
        Svc: WorkerBuilderExt<Args, Ctx, W, B, M> + Service<Task<Args, Ctx, B::IdType>>,
        // M::Service: Service<Task<Args, Ctx, IdType>, Response = U, Error = E>,
    {
        WorkerServiceBuilder::<B, Svc, Args, Ctx>::build(service, &self.source);
        // assert_worker(worker);
        // fn assert_worker<Args, Ctx, B, W, M>(_: Worker<Args, Ctx, B, W, M>) {
    }
}

/// Finalizes the builder and constructs a [`Worker`] with the provided service
impl<Args, Ctx, B, M> WorkerBuilder<Args, Ctx, B, M> {
    /// Consumes the builder and a service to construct the final worker
    pub fn build<W: WorkerBuilderExt<Args, Ctx, Svc, B, M>, Svc>(
        self,
        service: W,
    ) -> Worker<Args, Ctx, B, Svc, M> {
        service.with_builder(self)
    }
}

/// Trait for building a worker service provided a backend
pub trait WorkerServiceBuilder<Backend, Svc, Args, Ctx> {
    /// Build the service from the backend
    fn build(self, backend: &Backend) -> Svc;
}

/// Extension trait for building a worker from a builder
pub trait WorkerBuilderExt<Args, Ctx, Svc, Backend, M>: Sized {
    /// Consumes the builder and returns a worker
    fn with_builder(
        self,
        builder: WorkerBuilder<Args, Ctx, Backend, M>,
    ) -> Worker<Args, Ctx, Backend, Svc, M>;
}

impl<T, Args, Ctx, Svc, B, M> WorkerBuilderExt<Args, Ctx, Svc, B, M> for T
where
    T: WorkerServiceBuilder<B, Svc, Args, Ctx>,
    B: Backend<Args>,
{
    fn with_builder(
        self,
        builder: WorkerBuilder<Args, Ctx, B, M>,
    ) -> Worker<Args, Ctx, B, Svc, M> {
        let svc = self.build(&builder.source);
        let mut worker = Worker::new(builder.name, builder.source, svc, builder.layer);
        worker.event_handler = builder
            .event_handler
            .write()
            .map(|mut d| d.take())
            .unwrap()
            .unwrap_or(Box::new(|_ctx, _e| {
                trace!("Worker [{}] received event {_e}", _ctx.name());
            }));
        worker.shutdown = builder.shutdown;
        worker
    }
}
