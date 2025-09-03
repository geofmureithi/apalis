//! Traits and utilities for acknowledging task completion
//!
//! The [`Acknowledge`] trait and related types are responsible for adding custom
//! acknowledgment logic to workers. You can use [`AcknowledgeLayer`] to wrap
//! a worker service and invoke your acknowledgment handler after each task execution.
//!
//! # Example
//!
//! ```rust
//! # use apalis_core::worker::{builder::WorkerBuilder, ext::ack::{Acknowledge, AcknowledgeLayer}};
//! # use apalis_core::backend::memory::MemoryStorage;
//! # use apalis_core::worker::context::WorkerContext;
//! # use apalis_core::task::ExecutionContext;
//! # use apalis_core::error::BoxDynError;
//! # use futures_util::{future::{ready, BoxFuture}, FutureExt};
//! # use std::fmt::Debug;
//! # use tokio::sync::mpsc::error::SendError;
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut in_memory = MemoryStorage::new();
//!     in_memory.push(42).await.unwrap();
//!
//!     async fn task(
//!         task: u32,
//!         ctx: WorkerContext,
//!     ) -> Result<(), BoxDynError> {
//! #       ctx.stop().unwrap();
//!         Ok(())
//!     }
//!
//!     #[derive(Debug, Clone)]
//!     struct MyAcknowledger;
//!
//!     impl<Ctx: Debug, IdType: Debug> Acknowledge<(), Ctx, IdType> for MyAcknowledger {
//!         type Error = SendError<()>;
//!         type Future = BoxFuture<'static, Result<(), Self::Error>>;
//!         fn ack(
//!             &mut self,
//!             res: &Result<(), BoxDynError>,
//!             parts: &ExecutionContext<Ctx, IdType>,
//!         ) -> Self::Future {
//!             println!("{res:?}, {parts:?}");
//!             ready(Ok(())).boxed()
//!         }
//!     }
//!
//!     let worker = WorkerBuilder::new("rango-tango")
//!         .backend(in_memory)
//!         .ack_with(MyAcknowledger)
//!         .on_event(|ctx, ev| {
//!             println!("On Event = {:?}", ev);
//!         })
//!         .build(task);
//!     worker.run().await.unwrap();
//! }
//! ```
use futures_util::future::BoxFuture;
use std::{
    future::Future,
    task::Poll,
};
use tower_layer::{Layer, Stack};
use tower_service::Service;

use crate::{
    backend::Backend,
    error::BoxDynError,
    task::{ExecutionContext, Task},
    worker::{builder::WorkerBuilder, context::WorkerContext},
};

/// Extension trait for adding acknowledgment handling to workers
///
/// See [module level documentation](self) for more details.
pub trait AcknowledgementExt<Args, Meta, Source, Middleware, Ack, Res>: Sized
where
    Source: Backend<Args>,
    Ack: Acknowledge<Res, Meta, Source::IdType>,
{
    /// Add an acknowledgment handler to the worker
    fn ack_with(
        self,
        ack: Ack,
    ) -> WorkerBuilder<Args, Meta, Source, Stack<AcknowledgeLayer<Ack>, Middleware>>;
}

/// Acknowledge the result of a task processing
///
/// See [module level documentation](self) for more details.
pub trait Acknowledge<Res, Meta, IdType> {
    /// The error type returned by the acknowledgment process
    type Error;
    /// The future returned by the `ack` method
    type Future: Future<Output = Result<(), Self::Error>>;
    /// Acknowledge the result of a task processing
    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        ctx: &ExecutionContext<Meta, IdType>,
    ) -> Self::Future;
}

impl<Res, Meta, F, Fut, IdType, E> Acknowledge<Res, Meta, IdType> for F
where
    F: FnMut(&Result<Res, BoxDynError>, &ExecutionContext<Meta, IdType>) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    type Error = E;
    type Future = Fut;

    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        ctx: &ExecutionContext<Meta, IdType>,
    ) -> Self::Future {
        (self)(res, ctx)
    }
}

/// Layer that adds acknowledgment functionality to services
/// 
/// See [module level documentation](self) for more details.
#[derive(Debug, Clone)]
pub struct AcknowledgeLayer<A> {
    acknowledger: A,
}

impl<A> AcknowledgeLayer<A> {
    /// Create a new acknowledgment layer
    pub fn new(acknowledger: A) -> Self {
        Self { acknowledger }
    }
}

impl<S, A> Layer<S> for AcknowledgeLayer<A>
where
    A: Clone,
{
    type Service = AcknowledgeService<S, A>;

    fn layer(&self, inner: S) -> Self::Service {
        AcknowledgeService {
            inner,
            acknowledger: self.acknowledger.clone(),
        }
    }
}

/// Service that wraps another service and acknowledges task completion
/// 
/// See [module level documentation](self) for more details.

#[derive(Debug, Clone)]
pub struct AcknowledgeService<S, A> {
    inner: S,
    acknowledger: A,
}

impl<S, A, Args, Meta, Res, IdType> Service<Task<Args, Meta, IdType>> for AcknowledgeService<S, A>
where
    S: Service<Task<Args, Meta, IdType>, Response = Res>,
    A: Acknowledge<Res, Meta, IdType> + Clone + Send + 'static,
    S::Error: Into<BoxDynError>,
    A::Error: std::error::Error + Send + Sync + 'static,
    S::Future: Send + 'static,
    A::Future: Send + 'static,
    Meta: Clone + Sync + 'static + Send,
    Res: Send,
    IdType: Send + Clone + 'static,
{
    type Response = Res;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Res, BoxDynError>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: Task<Args, Meta, IdType>) -> Self::Future {
        let parts = req.ctx.clone();
        let worker: WorkerContext = req.ctx.data.get().cloned().unwrap();
        let future = self.inner.call(req);
        let mut acknowledger = self.acknowledger.clone();
        Box::pin(async move {
            let res = future.await.map_err(|e| e.into());
            worker.track(acknowledger.ack(&res, &parts)).await?; // Ensure ack is gracefully shutdown
            res
        })
    }
}

impl<Args, B, M, Meta, Ack, Res> AcknowledgementExt<Args, Meta, B, M, Ack, Res>
    for WorkerBuilder<Args, Meta, B, M>
where
    M: Layer<AcknowledgeLayer<Ack>>,
    Ack: Acknowledge<Res, Meta, B::IdType>,
    B: Backend<Args>,
{
    fn ack_with(self, ack: Ack) -> WorkerBuilder<Args, Meta, B, Stack<AcknowledgeLayer<Ack>, M>> {
        let this = self.layer(AcknowledgeLayer::new(ack));
        WorkerBuilder {
            name: this.name,
            request: this.request,
            layer: this.layer,
            source: this.source,
            shutdown: this.shutdown,
            event_handler: this.event_handler,
        }
    }
}
