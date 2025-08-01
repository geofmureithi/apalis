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
use std::{future::Future, marker::PhantomData};

use futures_util::Stream;
use tower::{
    layer::util::{Identity, Stack},
    util::BoxService,
    Layer, Service, ServiceBuilder,
};

use crate::{
    backend::{self, Backend},
    error::BoxDynError,
    monitor::shutdown::Shutdown,
    request::{data::Data, Request},
    service_fn::{into_response::IntoResponse, service_fn, ServiceFn},
    worker::{event::EventHandler, Worker},
};

/// Utility for building a [`Worker`]
pub struct WorkerBuilder<Args, Ctx, Source, Middleware> {
    pub(crate) name: String,
    pub(crate) request: PhantomData<(Args, Ctx)>,
    pub(crate) layer: ServiceBuilder<Middleware>,
    pub(crate) source: Source,
    pub(crate) event_handler: EventHandler,
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
            layer: ServiceBuilder::new(),
            source: (),
            name: name.as_ref().to_owned(),
            event_handler: EventHandler::default(),
            shutdown: None,
        }
    }
}

impl WorkerBuilder<(), (), (), Identity> {
    /// Consume a stream directly
    #[deprecated(since = "0.6.0", note = "Consider using the `.backend`")]
    pub fn stream<
        NS: Stream<Item = Result<Option<Request<NJ, Ctx>>, BoxDynError>> + Send + 'static,
        NJ,
        Ctx,
    >(
        self,
        stream: NS,
    ) -> WorkerBuilder<NJ, Ctx, NS, Identity> {
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
    pub fn backend<NB: Backend<NJ, Ctx>, NJ, Ctx>(
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

impl<Args, Ctx, M, B> WorkerBuilder<Args, Ctx, B, M> {
    /// Allows of decorating the service that consumes jobs.
    /// Allows adding multiple [`tower`] middleware
    pub fn chain<NewLayer>(
        self,
        f: impl FnOnce(ServiceBuilder<M>) -> ServiceBuilder<NewLayer>,
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
    pub fn data<D>(self, data: D) -> WorkerBuilder<Args, Ctx, B, Stack<Data<D>, M>>
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

impl<Args, Ctx, B, M> WorkerBuilder<Args, Ctx, B, M> {
    pub fn build<W: WorkerFactory<Args, Ctx, Svc, B, M>, Svc>(
        self,
        service: W,
    ) -> Worker<Args, Ctx, B, Svc, M> {
        service.factory(self)
    }
}

pub trait ServiceFactory<Resource, Svc, Args, Ctx> {
    fn service(self, resource: Resource) -> Svc;
}

// pub type BoxServiceFactory<Resource, Req, Res> =
//     Box<dyn ServiceFactory<Resource, BoxService<Req, Res, BoxDynError>>>;

pub trait WorkerFactory<Args, Ctx, Svc, Backend, M>: Sized {
    fn factory(
        self,
        builder: WorkerBuilder<Args, Ctx, Backend, M>,
    ) -> Worker<Args, Ctx, Backend, Svc, M>;
}

impl<T, Args, Ctx, Svc, B, M> WorkerFactory<Args, Ctx, Svc, B, M> for T
where
    T: ServiceFactory<B::Sink, Svc, Args, Ctx>,
    B: Backend<Args, Ctx>
{
    fn factory(
        self,
        builder: WorkerBuilder<Args, Ctx, B, M>,
    ) -> Worker<Args, Ctx, B, Svc, M> {
        let svc = self.service(builder.source.sink());
        let mut worker = Worker::new(builder.name, builder.source, svc, builder.layer);
        worker.event_handler = builder
            .event_handler
            .write()
            .map(|mut d| d.take())
            .unwrap()
            .unwrap_or(Box::new(|_, _| {}));
        worker.shutdown = builder.shutdown;
        worker
    }
}
