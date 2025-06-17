//! Represents utilities for building workers.
//!
//! This module provides the [`WorkerBuilder`] type, which is the recommended
//! way to construct [`Worker`](crate::worker::Worker) instances in a flexible and
//! composable manner.
//!
//! The builder pattern enables customization of various parts of a worker,
//! including:
//!
//! - Setting a backend that implements the [`Backend`](crate::backend::Backend) trait
//! - Adding application state (shared [`Data`](crate::request::data::Data))
//! - Decorating the service pipeline using [`tower`] middleware
//! - Handling lifecycle events with `on_event`
//! - Providing task processing logic using either a [`Service`] or an async function via `build_fn`
//!
//! ## Basic usage
//!
//! ```rust
//! use apalis_core::prelude::*;
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
//! # use tower::Service;
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
//! # use tower::Service;
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
//!     - `.chain(...)`: Composes middleware via `ServiceBuilder`.
//!     - `.build(service)`: Consumes the builder and a [`Service`] to construct the final [`Worker`].
//!     - `.build_fn(task_fn)`: Uses a function instead of a full [`Service`].
use std::marker::PhantomData;

use futures::Stream;
use tower::{
    layer::util::{Identity, Stack},
    Layer, Service, ServiceBuilder,
};

use crate::{
    backend::Backend,
    error::BoxDynError,
    monitor::shutdown::Shutdown,
    request::{data::Data, Request},
    service_fn::{service_fn, ServiceFn},
    worker::{event::EventHandler, Worker},
};

/// Utility for building a [`Worker`]
pub struct WorkerBuilder<Req, Source, Middleware> {
    pub(crate) name: String,
    pub(crate) request: PhantomData<Req>,
    pub(crate) layer: ServiceBuilder<Middleware>,
    pub(crate) source: Source,
    pub(crate) event_handler: EventHandler,
    pub(crate) shutdown: Option<Shutdown>,
}

impl<Req, Source, Middleware> std::fmt::Debug for WorkerBuilder<Req, Source, Middleware> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerBuilder")
            .field("id", &self.name)
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
            name: name.as_ref().to_owned(),
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
            name: self.name,
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
            name: self.name,
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
            name: self.name,
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
            name: self.name,
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
            name: self.name,
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
        let mut worker = Worker::new(self.name, self.source, service, self.layer);
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
