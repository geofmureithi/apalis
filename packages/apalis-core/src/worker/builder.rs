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
use std::{future::Future, marker::PhantomData};

use futures_util::Stream;
use tower_layer::{Identity, Layer, Stack};
use tower_service::Service;

use crate::{
    backend::Backend,
    monitor::shutdown::Shutdown,
    service_fn::into_response::IntoResponse,
    task::{data::Data, Task},
    worker::{event::EventHandler, Worker},
};

/// Utility for building a [`Worker`]
pub struct WorkerBuilder<Args, Meta, Source, Middleware> {
    pub(crate) name: String,
    pub(crate) request: PhantomData<(Args, Meta)>,
    pub(crate) layer: Middleware,
    pub(crate) source: Source,
    pub(crate) event_handler: EventHandler,
    pub(crate) shutdown: Option<Shutdown>,
}

impl<Args, Meta, Source, Middleware> std::fmt::Debug
    for WorkerBuilder<Args, Meta, Source, Middleware>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerBuilder")
            .field("id", &self.name)
            .field("job", &std::any::type_name::<(Args, Meta)>())
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
            event_handler: EventHandler::default(),
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

impl<Args, Meta, M, B> WorkerBuilder<Args, Meta, B, M>
where
    B: Backend<Args>,
{
    /// Allows of decorating the service that consumes jobs.
    /// Allows adding multiple [`tower`] middleware
    pub fn chain<NewLayer>(
        self,
        f: impl FnOnce(M) -> NewLayer,
    ) -> WorkerBuilder<Args, Meta, B, NewLayer> {
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
    pub fn layer<U>(self, layer: U) -> WorkerBuilder<Args, Meta, B, Stack<U, M>>
    where
        M: Layer<U>,
    {
        WorkerBuilder {
            request: self.request,
            source: self.source,
            layer: Stack::new(layer, self.layer), // TODO: Decide order here
            name: self.name,
            shutdown: self.shutdown,
            event_handler: self.event_handler,
        }
    }

    /// Adds data to the context
    /// This will be shared by all requests
    pub fn data<D>(self, data: D) -> WorkerBuilder<Args, Meta, B, Stack<Data<D>, M>>
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

    // pub fn check_worker<S, U, E, IdType>(self, service: S) -> Self

    #[inline]
    pub fn build_check<W, Svc>(self, service: Svc)
    where
        Svc: WorkerBuilderExt<Args, Meta, W, B, M> + Service<Task<Args, Meta, B::IdType>>,
         // M::Service: Service<Task<Args, Meta, IdType>, Response = U, Error = E>,
    {
        WorkerServiceBuilder::<B, Svc, Args, Meta>::build(service, &self.source);
        // assert_worker(worker);
        // fn assert_worker<Args, Meta, B, W, M>(_: Worker<Args, Meta, B, W, M>) {
    }
}

impl<Args, Meta, B, M> WorkerBuilder<Args, Meta, B, M> {
    pub fn build<W: WorkerBuilderExt<Args, Meta, Svc, B, M>, Svc>(
        self,
        service: W,
    ) -> Worker<Args, Meta, B, Svc, M> {
        service.with_builder(self)
    }
}

pub trait WorkerServiceBuilder<Backend, Svc, Args, Meta> {
    fn build(self, backend: &Backend) -> Svc;
}

pub trait WorkerBuilderExt<Args, Meta, Svc, Backend, M>: Sized {
    fn with_builder(
        self,
        builder: WorkerBuilder<Args, Meta, Backend, M>,
    ) -> Worker<Args, Meta, Backend, Svc, M>;
}

impl<T, Args, Meta, Svc, B, M> WorkerBuilderExt<Args, Meta, Svc, B, M> for T
where
    T: WorkerServiceBuilder<B, Svc, Args, Meta>,
    B: Backend<Args>,
{
    fn with_builder(self, builder: WorkerBuilder<Args, Meta, B, M>) -> Worker<Args, Meta, B, Svc, M> {
        let svc = self.build(&builder.source);
        let mut worker = Worker::new(builder.name, builder.source, svc, builder.layer);
        worker.event_handler = builder
            .event_handler
            .write()
            .map(|mut d| d.take())
            .unwrap()
            .unwrap_or(Box::new(|ctx, e| {
                trace!("Worker [{}] received event {e}", ctx.name());
            }));
        worker.shutdown = builder.shutdown;
        worker
    }
}
