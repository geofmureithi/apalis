//! Builder types for composing and building workers.
//!
//! The `WorkerBuilder` component is the recommended
//! way to construct [`Worker`] instances in a flexible and
//! composable manner.
//!
//! The builder pattern enables customization of various parts of a worker,
//! in the following order:
//!
//! 1. Setting a backend that implements the [`Backend`] trait
//! 2. Adding application state via [`Data`](crate::task::data)
//! 3. Decorating the service pipeline with middleware
//! 4. Handling lifecycle events with `on_event`
//! 5. Providing task processing logic using [`build`](WorkerBuilder::build) that implements [`IntoWorkerService`].
//!
//! The [`IntoWorkerService`] trait can be used to convert a function or a service into a worker service. The following implementations are provided:
//! - For async functions via [`task_fn`](crate::task_fn::task_fn)
//! - For any type that implements the [`Service`] trait for `T: Task`
//! - For workflows via [`apalis-workflow`](https://docs.rs/apalis-workflow)
//!
//! ## Basic usage
//!
//! ```rust,no_run
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_core::backend::memory::MemoryStorage;
//! # use apalis_core::worker::context::WorkerContext;
//! # use apalis_core::task::data::Data;
//! # use apalis_core::backend::TaskSink;
//! # use apalis_core::worker::ext::event_listener::EventListenerExt;
//!
//! # #[tokio::main]
//! # async fn main() {
//! # let mut in_memory = MemoryStorage::new();
//! # in_memory.push(24).await.unwrap();
//! async fn task(job: u32, count: Data<usize>, ctx: WorkerContext) {
//!     println!("Received job: {job:?}");
//!     ctx.stop().unwrap();
//! }
//!
//! let worker = WorkerBuilder::new("rango-tango")
//!     .backend(in_memory)
//!     .data(0usize)
//!     .on_event(|ctx, ev| {
//!         println!("On Event = {:?}", ev);
//!     })
//!     .build(task);
//!
//! worker.run().await.unwrap();
//! # }
//! ```
//! ## Order
//!
//! The order in which you add layers affects how tasks are processed. Layers added earlier are wrapped by those added later.
//!
//! ### Why does order matter?
//! Each layer wraps the previous one, so the outermost layer is applied last. This means that middleware added later can observe or modify the effects of earlier layers. For example, tracing added before retry will see all retries as a single operation, while tracing added after retry will log each retry attempt separately.
//!
//! For example:
//! ```ignore
//! WorkerBuilder::new()
//!     .enable_tracing()
//!     .retry(RetryPolicy::retries(3))
//!     .build(task);
//! ```
//! In this case, tracing is applied before retry. The tracing span may not reflect the correct attempt count.
//!
//! Reversing the order:
//! ```ignore
//! WorkerBuilder::new()
//!     .retry(RetryPolicy::retries(3))
//!     .enable_tracing()
//!     .build(task);
//! ```
//! Now, retry is applied first, and tracing wraps around it. The tracing span will correctly capture retries.
//!
//! **Tip:** Add layers in the order you want them to wrap task processing.
use std::marker::PhantomData;
use tower_layer::{Identity, Layer, Stack};
use tower_service::Service;

use crate::{
    backend::Backend,
    monitor::shutdown::Shutdown,
    task::{Task, data::Data},
    worker::{Worker, event::EventHandlerBuilder},
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
    pub fn backend<NB, NJ, Ctx>(self, backend: NB) -> WorkerBuilder<NJ, Ctx, NB, Identity>
    where
        NB: Backend<Args = NJ, Context = Ctx>,
    {
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
    B: Backend<Args = Args>,
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
    /// Allows adding middleware to the layer stack
    pub fn layer<U>(self, layer: U) -> WorkerBuilder<Args, Ctx, B, Stack<U, M>> {
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
}

/// Finalizes the builder and constructs a [`Worker`] with the provided service
impl<Args, Ctx, B, M> WorkerBuilder<Args, Ctx, B, M>
where
    B: Backend<Args = Args, Context = Ctx>,
{
    /// Consumes the builder and a service to construct the final worker
    pub fn build<W: IntoWorkerServiceExt<Args, Ctx, Svc, B, M>, Svc>(
        self,
        service: W,
    ) -> Worker<Args, Ctx, B, Svc, M>
    where
        Svc: Service<Task<Args, Ctx, B::IdType>>,
    {
        service.build_with(self)
    }
}

/// Trait for building a worker service provided a backend
pub trait IntoWorkerService<Backend, Svc, Args, Ctx>
where
    Backend: crate::backend::Backend<Args = Args, Context = Ctx>,
    Svc: Service<Task<Args, Ctx, Backend::IdType>>,
{
    /// Build the service from the backend
    fn into_service(self, backend: &Backend) -> Svc;
}

/// Extension trait for building a worker from a builder
pub trait IntoWorkerServiceExt<Args, Ctx, Svc, Backend, M>: Sized
where
    Backend: crate::backend::Backend<Args = Args, Context = Ctx>,
    Svc: Service<Task<Args, Ctx, Backend::IdType>>,
{
    /// Consumes the builder and returns a worker
    fn build_with(
        self,
        builder: WorkerBuilder<Args, Ctx, Backend, M>,
    ) -> Worker<Args, Ctx, Backend, Svc, M>;
}

/// Implementation of the IntoWorkerServiceExt trait for any type
///
/// Rust doest offer specialization yet, the [`IntoWorkerServiceExt`] and [`IntoWorkerService`]
/// traits are used to allow the [build](WorkerBuilder::build) method to be more flexible.
impl<T, Args, Ctx, Svc, B, M> IntoWorkerServiceExt<Args, Ctx, Svc, B, M> for T
where
    T: IntoWorkerService<B, Svc, Args, Ctx>,
    B: Backend<Args = Args, Context = Ctx>,
    Svc: Service<Task<Args, Ctx, B::IdType>>,
{
    fn build_with(self, builder: WorkerBuilder<Args, Ctx, B, M>) -> Worker<Args, Ctx, B, Svc, M> {
        let svc = self.into_service(&builder.source);
        let mut worker = Worker::new(builder.name, builder.source, svc, builder.layer);
        worker.event_handler = builder
            .event_handler
            .write()
            .map(|mut d| d.take())
            .unwrap()
            .unwrap_or(Box::new(|_ctx, _e| {
                debug!("Worker [{}] received event {_e}", _ctx.name());
            }));
        worker.shutdown = builder.shutdown;
        worker
    }
}

macro_rules! impl_check_fn {
    ($($num:tt => $($arg:ident),+);+ $(;)?) => {
        $(
            #[inline]
            #[doc = concat!("A helper for checking that the builder can build a worker with the provided service (", stringify!($num), " arguments)")]
            pub fn $num<
                F,
                $($arg: FromRequest<Task<Args, Ctx, B::IdType>>),+
            >(
                self,
                _: F,
            ) where
                TaskFn<F, Args, Ctx, ($($arg,)+)>: Service<Task<Args, Ctx, B::IdType>>,
            {
            }
        )+
    };
}

use crate::task_fn::{FromRequest, TaskFn};

impl<Args, Ctx, M, B> WorkerBuilder<Args, Ctx, B, M>
where
    B: Backend<Args = Args>,
{
    impl_check_fn! {
        check_fn => A1;
        check_fn_2 => A1, A2;
        check_fn_3 => A1, A2, A3;
        check_fn_4 => A1, A2, A3, A4;
        check_fn_5 => A1, A2, A3, A4, A5;
        check_fn_6 => A1, A2, A3, A4, A5, A6;
        check_fn_7 => A1, A2, A3, A4, A5, A6, A7;
        check_fn_8 => A1, A2, A3, A4, A5, A6, A7, A8;
        check_fn_9 => A1, A2, A3, A4, A5, A6, A7, A8, A9;
        check_fn_10 => A1, A2, A3, A4, A5, A6, A7, A8, A9, A10;
        check_fn_11 => A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11;
        check_fn_12 => A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12;
        check_fn_13 => A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13;
        check_fn_14 => A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14;
        check_fn_15 => A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15;
        check_fn_16 => A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16;
    }
}
