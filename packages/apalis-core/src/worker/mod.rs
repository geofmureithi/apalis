//! Utilities for building and running workers.
//!
//! A `Worker` polls tasks from a backend, executes them using
//! a service, emits lifecycle events, and handles graceful shutdowns. A worker is typically
//! constructed using a [`WorkerBuilder`](crate::worker::builder).
//!
//! # Features
//! - Pluggable backends for task queues (e.g., in-memory, Redis).
//! - Middleware support for request processing.
//! - Stream or future-based worker execution modes.
//! - Built-in event system for logging or metrics.
//! - Job tracking and readiness probing.
//!
//! # Lifecycle
//!
//! ```mermaid
//! graph TD
//!     A[Start Worker] --> B[Initialize Context & Heartbeat]
//!     B --> C[Poll Backend for Tasks]
//!     C --> D{Task Available?}
//!     D -- Yes --> E[Execute Task via Service Stack]
//!     E --> F[Emit Events]
//!     F --> C
//!     D -- No --> F
//!     F --> G{Shutdown Signal?}
//!     G -- Yes --> H[Graceful Shutdown]
//!     G -- No --> C
//! ```
//! Worker lifecycle is composed of several stages:
//! - Initialize context and heartbeat
//! - Poll backend for tasks
//! - Execute tasks via service stack
//! - Emit events (Idle, Success, Error, HeartBeat)
//! - Graceful shutdown on signal or stop
//!
//! # Examples
//!
//! ## Run as a future
//! ```rust,no_run
//! # use apalis_core::{worker::builder::WorkerBuilder, backend::memory::MemoryStorage};
//! # use apalis_core::error::BoxDynError;
//! # use apalis_core::backend::TaskSink;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), BoxDynError> {
//!     let mut storage = MemoryStorage::new();
//!     for i in 0..5 {
//!         storage.push(i).await?;
//!     }
//!
//!     async fn handler(task: u32) {
//!         println!("Processing task: {task}");
//!     }
//!
//!     let worker = WorkerBuilder::new("worker-1")
//!         .backend(storage)
//!         .build(handler);
//!
//!     worker.run().await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Runner as a stream
//! The `stream` interface yields worker events (e.g., `Success`, `Error`) while running:
//! ```rust,no_run
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_core::backend::memory::MemoryStorage;
//! # use futures_util::StreamExt;
//! # #[tokio::main]
//! # async fn main() {
//! #   let mut storage = MemoryStorage::new();
//! #   async fn handler(task: u32) {
//! #        println!("Processing task: {task}");
//! #    }
//! #   let worker = WorkerBuilder::new("worker-1")
//! #        .backend(storage)
//! #        .build(handler);
//! let mut stream = worker.stream();
//! while let Some(evt) = stream.next().await {
//!     println!("Event: {:?}", evt);
//! }
//! # }
//! ```
//!
//! # Test Utilities
//! The [`test_worker`] module includes utilities for unit tests and validation of worker behavior.
use crate::backend::Backend;
use crate::error::{BoxDynError, WorkerError};
use crate::monitor::shutdown::Shutdown;
use crate::task::Task;
use crate::task::attempt::Attempt;
use crate::task::data::Data;
use crate::worker::call_all::{CallAllError, CallAllUnordered};
use crate::worker::context::{Tracked, WorkerContext};
use crate::worker::event::{Event, RawEventListener};
use futures_core::stream::BoxStream;
use futures_util::{Future, FutureExt, Stream, StreamExt, TryFutureExt};
use std::fmt::Debug;
use std::fmt::{self};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use tower_layer::{Layer, Stack};
use tower_service::Service;

pub mod builder;
pub mod call_all;
pub mod context;
pub mod event;
pub mod ext;
mod state;
pub mod test_worker;

/// Core component responsible for task polling, execution, and lifecycle management.
///
/// # Example
/// Basic example:
/// ```rust,no_run
/// # use apalis_core::error::BoxDynError;
/// # use apalis_core::backend::memory::MemoryStorage;
/// # use apalis_core::worker::builder::WorkerBuilder;
/// # use apalis_core::backend::TaskSink;
///
/// #[tokio::main]
/// async fn main() -> Result<(), BoxDynError> {
///     let mut storage = MemoryStorage::new();
///     for i in 0..5 {
///         storage.push(i).await?;
///     }
///
///     async fn handler(task: u32) {
///         println!("Processing task: {task}");
///     }
///
///     let worker = WorkerBuilder::new("worker-1")
///         .backend(storage)
///         .build(handler);
///
///     worker.run().await?;
///     Ok(())
/// }
/// ```
/// See [module level documentation](self) for more details.
#[must_use = "Workers must be run or streamed to execute tasks"]
pub struct Worker<Args, Ctx, Backend, Svc, Middleware> {
    pub(crate) name: String,
    pub(crate) backend: Backend,
    pub(crate) service: Svc,
    pub(crate) middleware: Middleware,
    pub(crate) task_marker: PhantomData<(Args, Ctx)>,
    pub(crate) shutdown: Option<Shutdown>,
    pub(crate) event_handler: RawEventListener,
}

impl<Args, Ctx, B, Svc, Middleware> fmt::Debug for Worker<Args, Ctx, B, Svc, Middleware>
where
    Svc: fmt::Debug,
    B: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Worker")
            .field("service", &self.service)
            .field("backend", &self.backend)
            .finish()
    }
}

impl<Args, Ctx, B, Svc, M> Worker<Args, Ctx, B, Svc, M> {
    /// Build a worker that is ready for execution
    pub fn new(name: String, backend: B, service: Svc, layers: M) -> Self {
        Worker {
            name,
            backend,
            service,
            middleware: layers,
            task_marker: PhantomData,
            shutdown: None,
            event_handler: Box::new(|_, _| {}),
        }
    }
}

impl<Args, S, B, M> Worker<Args, B::Context, B, S, M>
where
    B: Backend<Args = Args>,
    S: Service<Task<Args, B::Context, B::IdType>> + Send + 'static,
    B::Stream: Unpin + Send + 'static,
    B::Beat: Unpin + Send + 'static,
    Args: Send + 'static,
    B::Context: Send + 'static,
    B::Error: Into<BoxDynError> + Send + 'static,
    B::Layer: Layer<ReadinessService<TrackerService<S>>>,
    M: Layer<<<B as Backend>::Layer as Layer<ReadinessService<TrackerService<S>>>>::Service>,
    M::Service: Service<Task<Args, B::Context, B::IdType>> + Send + 'static,
    <M::Service as Service<Task<Args, B::Context, B::IdType>>>::Error:
        Into<BoxDynError> + Send + Sync + 'static,
    <M::Service as Service<Task<Args, B::Context, B::IdType>>>::Future: Send,
    B::IdType: Send + 'static,
{
    /// Run the worker until completion
    ///
    /// # Example
    /// ```no_run
    /// # use apalis_core::error::BoxDynError;
    /// # use apalis_core::backend::memory::MemoryStorage;
    /// # use apalis_core::backend::TaskSink;
    /// # use apalis_core::worker::builder::WorkerBuilder;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), BoxDynError> {
    ///     let mut storage = MemoryStorage::new();
    ///     for i in 0..5 {
    ///         storage.push(i).await?;
    ///     }
    ///
    ///     async fn handler(task: u32) {
    ///         println!("Processing task: {task}");
    ///     }
    ///
    ///     let worker = WorkerBuilder::new("worker-1")
    ///         .backend(storage)
    ///         .build(handler);
    ///
    ///     worker.run().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn run(self) -> Result<(), WorkerError> {
        let mut ctx = WorkerContext::new::<M::Service>(&self.name);
        self.run_with_ctx(&mut ctx).await
    }

    /// Run the worker with the given context.
    ///
    /// See [`run`](Self::run) for an example.
    pub async fn run_with_ctx(self, ctx: &mut WorkerContext) -> Result<(), WorkerError> {
        let mut stream = self.stream_with_ctx(ctx);
        while let Some(res) = stream.next().await {
            match res {
                Ok(_) => continue,
                Err(WorkerError::GracefulExit) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Run the worker with a shutdown signal future.
    pub async fn run_with<Fut>(mut self, signal: Fut) -> Result<(), WorkerError>
    where
        Fut: Future<Output = Result<(), WorkerError>> + Send + 'static,
        B: Send,
        M: Send,
    {
        let shutdown = self.shutdown.take().unwrap_or(Shutdown::new());
        let terminator = shutdown.shutdown_after(signal);
        let mut ctx = WorkerContext::new::<M::Service>(&self.name);
        let c = ctx.clone();
        let worker = self.run_with_ctx(&mut ctx).boxed();
        futures_util::try_join!(terminator.map_ok(|_| c.stop()), worker).map(|_| ())
    }

    /// Returns a stream that will yield events as they occur within the worker's lifecycle
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use apalis_core::error::BoxDynError;
    /// # use apalis_core::backend::memory::MemoryStorage;
    /// # use apalis_core::worker::builder::WorkerBuilder;
    /// # use apalis_core::backend::TaskSink;
    /// # use futures_util::StreamExt;
    /// #[tokio::main]
    /// async fn main() -> Result<(), BoxDynError> {
    ///     let mut storage = MemoryStorage::new();
    ///     for i in 0..5 {
    ///         storage.push(i).await?;
    ///     }
    ///     async fn handler(task: u32) {
    ///         println!("Processing task: {task}");
    ///     }
    ///     let worker = WorkerBuilder::new("worker-1")
    ///         .backend(storage)
    ///         .build(handler);
    ///     let mut stream = worker.stream();
    ///     while let Some(evt) = stream.next().await {
    ///         println!("Event: {:?}", evt);
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn stream(self) -> impl Stream<Item = Result<Event, WorkerError>> + use<Args, S, B, M> {
        let mut ctx = WorkerContext::new::<M::Service>(&self.name);
        self.stream_with_ctx(&mut ctx)
    }

    /// Returns a stream that will yield events as they occur within the worker's lifecycle when provided
    /// with a [`WorkerContext`].
    ///
    /// See [`stream`](Self::stream) for an example.
    pub fn stream_with_ctx(
        self,
        ctx: &mut WorkerContext,
    ) -> impl Stream<Item = Result<Event, WorkerError>> + use<Args, S, B, M> {
        let backend = self.backend;
        let event_handler = self.event_handler;
        ctx.wrap_listener(event_handler);
        let worker = ctx.clone();
        let inner_layers = backend.middleware();
        struct ServiceBuilder<L> {
            layer: L,
        }

        impl<L> ServiceBuilder<L> {
            fn layer<T>(self, layer: T) -> ServiceBuilder<Stack<T, L>> {
                ServiceBuilder {
                    layer: Stack::new(layer, self.layer),
                }
            }
            fn service<S>(&self, service: S) -> L::Service
            where
                L: Layer<S>,
            {
                self.layer.layer(service)
            }
        }
        let svc = ServiceBuilder {
            layer: Data::new(worker.clone()),
        };
        let service = svc
            .layer(self.middleware)
            .layer(inner_layers)
            .layer(ReadinessLayer::new(worker.clone()))
            .layer(TrackerLayer::new(worker.clone()))
            .service(self.service);
        let heartbeat = backend.heartbeat(&worker).map(|res| match res {
            Ok(_) => Ok(Event::HeartBeat),
            Err(e) => Err(WorkerError::HeartbeatError(e.into())),
        });

        let stream = backend.poll(&worker);

        let tasks = Self::poll_tasks(service, stream);
        let mut w = worker.clone();
        let mut ww = w.clone();
        let starter: BoxStream<'static, _> = futures_util::stream::once(async move {
            if !ww.is_running() {
                ww.start()?;
            }
            Ok(None)
        })
        .filter_map(|res: Result<Option<Event>, WorkerError>| async move {
            match res {
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            }
        })
        .boxed();
        let wait_for_exit: BoxStream<'static, _> = futures_util::stream::once(async move {
            match worker.await {
                Ok(_) => Err(WorkerError::GracefulExit),
                Err(e) => Err(e),
            }
        })
        .boxed();
        let work_stream =
            futures_util::stream_select!(wait_for_exit, heartbeat, tasks).map(move |res| {
                if let Ok(e) = &res {
                    w.emit(e);
                }
                res
            });
        starter.chain(work_stream)
    }
    fn poll_tasks<Svc, Stm, E, Ctx>(
        service: Svc,
        stream: Stm,
    ) -> BoxStream<'static, Result<Event, WorkerError>>
    where
        Svc: Service<Task<Args, Ctx, B::IdType>> + Send + 'static,
        Stm: Stream<Item = Result<Option<Task<Args, Ctx, B::IdType>>, E>> + Send + Unpin + 'static,
        Args: Send + 'static,
        Svc::Future: Send,
        Ctx: Send + 'static,
        Svc::Error: Into<BoxDynError> + Sync + Send,
        E: Into<BoxDynError> + Send + 'static,
    {
        let stream = CallAllUnordered::new(service, stream).map(|r| match r {
            Ok(Some(_)) => Ok(Event::Success),
            Ok(None) => Ok(Event::Idle),
            Err(CallAllError::ServiceError(err)) => Ok(Event::Error(err.into().into())),
            Err(CallAllError::StreamError(err)) => Err(WorkerError::StreamError(err)),
        });
        stream.boxed()
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
/// Service that tracks a tasks future allowing graceful shutdowns
#[derive(Debug, Clone)]
pub struct TrackerService<S> {
    ctx: WorkerContext,
    service: S,
}

impl<S, Args, Ctx, IdType> Service<Task<Args, Ctx, IdType>> for TrackerService<S>
where
    S: Service<Task<Args, Ctx, IdType>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Tracked<AttemptOnPollFuture<S::Future>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, task: Task<Args, Ctx, IdType>) -> Self::Future {
        let attempt = task.parts.attempt.clone();
        self.ctx.track(AttemptOnPollFuture {
            attempt,
            fut: self.service.call(task),
            polled: false,
        })
    }
}

/// A future that increments the attempt count on the first poll
#[pin_project::pin_project]
#[derive(Debug)]
pub struct AttemptOnPollFuture<Fut> {
    attempt: Attempt,
    #[pin]
    fut: Fut,
    polled: bool,
}

impl<Fut> AttemptOnPollFuture<Fut> {
    /// Create a new attempt on poll future
    pub fn new(attempt: Attempt, fut: Fut) -> Self {
        Self {
            attempt,
            fut,
            polled: false,
        }
    }
}

impl<Fut: Future> Future for AttemptOnPollFuture<Fut> {
    type Output = Fut::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if !(*this.polled) {
            *this.polled = true;
            this.attempt.increment();
        }
        this.fut.poll_unpin(cx)
    }
}

/// Injects the [`ReadinessService`] to track when workers are ready to accept new tasks
#[derive(Debug, Clone)]
struct ReadinessLayer {
    ctx: WorkerContext,
}

impl ReadinessLayer {
    fn new(ctx: WorkerContext) -> Self {
        Self { ctx }
    }
}

impl<S> Layer<S> for ReadinessLayer {
    type Service = ReadinessService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ReadinessService {
            inner,
            ctx: self.ctx.clone(),
        }
    }
}
/// Service that tracks the readiness of underlying services
///
/// Should be the innermost service
#[derive(Debug, Clone)]
pub struct ReadinessService<S> {
    inner: S,
    ctx: WorkerContext,
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
            Poll::Ready(Ok(_)) => self.ctx.is_ready.store(true, Ordering::SeqCst),
            Poll::Pending | Poll::Ready(Err(_)) => self.ctx.is_ready.store(false, Ordering::SeqCst),
        }

        result
    }

    fn call(&mut self, req: Request) -> Self::Future {
        self.inner.call(req)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::ready,
        ops::Deref,
        sync::{Arc, atomic::AtomicUsize},
        time::Duration,
    };

    use futures_channel::mpsc::SendError;
    use futures_core::future::BoxFuture;

    use crate::{
        backend::{TaskSink, json::JsonStorage, memory::MemoryStorage},
        task::Parts,
        worker::{
            builder::WorkerBuilder,
            ext::{
                ack::{Acknowledge, AcknowledgementExt},
                circuit_breaker::CircuitBreaker,
                event_listener::EventListenerExt,
                long_running::LongRunningExt,
            },
        },
    };

    use super::*;

    const ITEMS: u32 = 100;

    #[tokio::test]
    async fn basic_worker_run() {
        let mut json_store = JsonStorage::new_temp().unwrap();
        for i in 0..ITEMS {
            json_store.push(i).await.unwrap();
        }

        #[derive(Clone, Debug, Default)]
        struct Count(Arc<AtomicUsize>);

        impl Deref for Count {
            type Target = Arc<AtomicUsize>;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        async fn task(
            task: u32,
            count: Data<Count>,
            ctx: WorkerContext,
        ) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            count.fetch_add(1, Ordering::Relaxed);
            if task == ITEMS - 1 {
                ctx.stop().unwrap();
                return Err("Worker stopped!")?;
            }
            Ok(())
        }

        #[derive(Debug, Clone)]
        struct MyAcknowledger;

        impl<Ctx: Debug, IdType: Debug> Acknowledge<(), Ctx, IdType> for MyAcknowledger {
            type Error = SendError;
            type Future = BoxFuture<'static, Result<(), SendError>>;
            fn ack(
                &mut self,
                res: &Result<(), BoxDynError>,
                parts: &Parts<Ctx, IdType>,
            ) -> Self::Future {
                println!("{res:?}, {parts:?}");
                // Call webhook with the result and parts?
                ready(Ok(())).boxed()
            }
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(json_store)
            .data(Count::default())
            .break_circuit()
            .long_running()
            .ack_with(MyAcknowledger)
            .on_event(|ctx, ev| {
                println!("On Event = {:?} from {}", ev, ctx.name());
            })
            .build(task);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn basic_worker_stream() {
        let mut in_memory = MemoryStorage::new();

        for i in 0..ITEMS {
            in_memory.push(i).await.unwrap();
        }

        #[derive(Clone, Debug, Default)]
        struct Count(Arc<AtomicUsize>);

        impl Deref for Count {
            type Target = Arc<AtomicUsize>;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        async fn task(task: u32, count: Data<Count>, worker: WorkerContext) {
            tokio::time::sleep(Duration::from_secs(1)).await;
            count.fetch_add(1, Ordering::Relaxed);
            if task == ITEMS - 1 {
                worker.stop().unwrap();
            }
        }
        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .data(Count::default())
            .break_circuit()
            .long_running()
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
            })
            .build(task);
        let mut event_stream = worker.stream();
        while let Some(Ok(ev)) = event_stream.next().await {
            println!("On Event = {:?}", ev);
        }
    }

    #[tokio::test]
    async fn with_shutdown_signal() {
        let mut in_memory = MemoryStorage::new();
        for i in 0..ITEMS {
            in_memory.push(i).await.unwrap();
        }

        async fn task(_: u32) -> Result<(), BoxDynError> {
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .on_event(|ctx, ev| {
                println!("On Event = {:?} from {}", ev, ctx.name());
            })
            .build(task);
        let signal = async {
            let ctrl_c = tokio::signal::ctrl_c().map_err(|e| e.into());
            let timeout = tokio::time::sleep(Duration::from_secs(5))
                .map(|_| Err::<(), WorkerError>(WorkerError::GracefulExit));
            let _ = futures_util::try_join!(ctrl_c, timeout)?;
            Ok(())
        };
        let res = worker.run_with(signal).await;
        match res {
            Err(WorkerError::GracefulExit) => {
                println!("Worker exited gracefully");
            }
            _ => panic!("Expected graceful exit error"),
        }
    }
}
