//! Worker extension for parallel execution and spawning child tasks.
//!
//! This module provides functionality to parallelize task execution within a worker
//! and spawn child tasks. It includes the [`ParallelizeLayer`] middleware for parallel execution
//! and the [`ParallelizeService`] service.
//!
//! # Example
//! ```rust,no_run
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_core::backend::memory::MemoryStorage;
//! # use apalis_core::worker::ext::parallelize::ParallelizeExt;
//! # use apalis_core::worker::ext::event_listener::EventListenerExt;
//! #[tokio::main]
//! async fn main() {
//!     async fn task(task: u32) {
//!         println!("Processing task: {task}");
//!     }
//!     let in_memory = MemoryStorage::new();
//!     let worker = WorkerBuilder::new("rango-tango")
//!         .backend(in_memory)
//!         .parallelize(tokio::spawn)
//!         .on_event(|ctx, ev| {
//!             println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
//!         })
//!         .build(task);
//!     worker.run().await.unwrap();
//! }
//! ```
use std::future::ready;

use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use futures_util::TryFutureExt;
use tower_layer::{Layer, Stack};
use tower_service::Service;

use crate::{backend::Backend, error::BoxDynError, task::Task, worker::builder::WorkerBuilder};

/// Worker extension for parallel execution
pub trait ParallelizeExt<Args, Ctx, Source, Middleware, Executor>: Sized {
    /// Register the executor for parallel task execution.
    fn parallelize(
        self,
        f: Executor,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<ParallelizeLayer<Executor>, Middleware>>;
}

/// Middleware for emitting events
#[derive(Debug, Clone, Default)]
pub struct ParallelizeLayer<Executor> {
    executor: Executor,
}

impl<Executor> ParallelizeLayer<Executor> {
    /// Create a new event listener layer
    pub fn new(executor: Executor) -> Self {
        Self { executor }
    }
}

impl<S, Executor: Clone> Layer<S> for ParallelizeLayer<Executor> {
    type Service = ParallelizeService<S, Executor>;

    fn layer(&self, service: S) -> Self::Service {
        ParallelizeService {
            service,
            executor: self.executor.clone(),
        }
    }
}

/// Service for emitting events
#[derive(Debug, Clone)]
pub struct ParallelizeService<S, Executor> {
    service: S,
    executor: Executor,
}

impl<S, Args, Ctx, IdType, Fut, T, Executor, ExecErr> Service<Task<Args, Ctx, IdType>>
    for ParallelizeService<S, Executor>
where
    S: Service<Task<Args, Ctx, IdType>, Future = Fut>,
    Executor: Fn(Fut) -> T + Send + 'static,
    Fut: Future<Output = Result<S::Response, S::Error>> + Send + 'static,
    T: Future<Output = Result<Result<S::Response, S::Error>, ExecErr>> + Send + 'static,
    S::Error: Into<BoxDynError> + Send + 'static,
    ExecErr: Into<BoxDynError>,
    S::Response: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, request: Task<Args, Ctx, IdType>) -> Self::Future {
        (self.executor)(self.service.call(request))
            .map_err(|e| e.into())
            .and_then(|s| ready(s.map_err(|e| e.into())))
            .boxed()
    }
}

impl<Args, P, M, Ctx, Executor> ParallelizeExt<Args, Ctx, P, M, Executor>
    for WorkerBuilder<Args, Ctx, P, M>
where
    P: Backend<Args = Args, Context = Ctx>,
    M: Layer<ParallelizeLayer<Executor>>,
{
    fn parallelize(
        self,
        f: Executor,
    ) -> WorkerBuilder<Args, Ctx, P, Stack<ParallelizeLayer<Executor>, M>> {
        self.layer(ParallelizeLayer::new(f))
    }
}
