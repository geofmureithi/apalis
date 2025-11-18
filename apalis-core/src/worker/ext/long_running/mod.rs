//! # Extension traits for long running tasks
//!
//! It includes a tracker for monitoring task duration and a middleware layer to integrate with the worker's service stack.
//! The long-running task support ensures that tasks exceeding a specified duration are properly tracked and managed, allowing for graceful shutdown and resource cleanup.
//!
//! ## Features
//! - [`TaskTracker`]: Monitors the duration of tasks and provides a mechanism to wait for their completion.
//! - [`LongRunningLayer`]: A Tower middleware layer that wraps the worker's service to add long-running task tracking capabilities.
//! - [`LongRunnerCtx`]: A context object that can be injected into tasks to allow them to register long-running operations.
//! - [`LongRunningExt`]: Provides an extension trait for easily adding long-running support to workers.
//!
//! ## Example
//!
//! ```rust
//! # use apalis_core::worker::ext::long_running::{LongRunnerCtx, LongRunningExt};
//! # use apalis_core::worker::context::WorkerContext;
//! # use apalis_core::backend::memory::MemoryStorage;
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use std::time::Duration;
//! # use crate::apalis_core::backend::TaskSink;
//! # use apalis_core::error::BoxDynError;
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut in_memory = MemoryStorage::new();
//!     in_memory.push(42).await.unwrap();
//!
//!     async fn task(
//!         task: u32,
//!         runner: LongRunnerCtx,
//! #        worker: WorkerContext,
//!     ) -> Result<(), BoxDynError> {
//!         // Spawn and track the long-running task
//!         tokio::spawn(runner.track(async move {
//!             tokio::time::sleep(Duration::from_secs(1)).await;
//!         }));
//! #        tokio::spawn(async move {
//! #            tokio::time::sleep(Duration::from_secs(1)).await;
//! #            worker.stop().unwrap();
//! #        });
//!         Ok(())
//!     }
//!
//!     let worker = WorkerBuilder::new("rango-tango")
//!         .backend(in_memory)
//!         .long_running()
//!         .build(task);
//!     worker.run().await.unwrap();
//! }
//! ```
use std::{future::Future, time::Duration};

use futures_util::{FutureExt, future::BoxFuture};
use tower_layer::{Layer, Stack};
use tower_service::Service;

use crate::{
    backend::Backend,
    task::{Task, data::MissingDataError},
    task_fn::FromRequest,
    worker::{
        builder::WorkerBuilder,
        context::{Tracked, WorkerContext},
        ext::long_running::tracker::{LongRunningFuture, TaskTracker},
    },
};

pub mod tracker;

/// Represents the long running middleware config
///
/// See [module level documentation](self) for more details.
#[derive(Debug, Clone, Default)]
pub struct LongRunningConfig {
    #[allow(unused)]
    max_duration: Option<Duration>,
}
impl LongRunningConfig {
    /// Create a new long running config
    #[must_use]
    pub fn new(max_duration: Duration) -> Self {
        Self {
            max_duration: Some(max_duration),
        }
    }
}

/// The long running middleware context
///
/// See [module level documentation](self) for more details.
#[derive(Debug, Clone)]
pub struct LongRunnerCtx {
    tracker: TaskTracker,
    wrk: WorkerContext,
}

impl LongRunnerCtx {
    /// Start a task that is tracked by the long running task's context
    pub fn track<F: Future>(&self, task: F) -> Tracked<LongRunningFuture<F>> {
        self.wrk.track(self.tracker.track_future(task))
    }
}

impl<Args: Sync, Ctx: Sync + Clone, IdType: Sync + Send> FromRequest<Task<Args, Ctx, IdType>>
    for LongRunnerCtx
{
    type Error = MissingDataError;
    async fn from_request(task: &Task<Args, Ctx, IdType>) -> Result<Self, Self::Error> {
        let tracker: &TaskTracker = task.parts.data.get_checked()?;
        let wrk: &WorkerContext = task.parts.data.get_checked()?;
        Ok(Self {
            tracker: tracker.clone(),
            wrk: wrk.clone(),
        })
    }
}

/// Decorates the underlying middleware with long running capabilities
///
/// See [module level documentation](self) for more details.
#[derive(Debug, Clone)]
#[allow(unused)]
pub struct LongRunningLayer(LongRunningConfig);

impl LongRunningLayer {
    /// Create a new long running layer
    #[must_use]
    pub fn new(config: LongRunningConfig) -> Self {
        Self(config)
    }
}

impl<S> Layer<S> for LongRunningLayer {
    type Service = LongRunningService<S>;

    fn layer(&self, service: S) -> Self::Service {
        LongRunningService { service }
    }
}

/// Decorates the underlying service with long running capabilities
///
/// See [module level documentation](self) for more details.
#[derive(Debug, Clone)]
pub struct LongRunningService<S> {
    service: S,
}

impl<S, Args, Ctx, IdType> Service<Task<Args, Ctx, IdType>> for LongRunningService<S>
where
    S: Service<Task<Args, Ctx, IdType>>,
    S::Future: Send + 'static,
    S::Response: Send,
    S::Error: Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<S::Response, S::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, mut task: Task<Args, Ctx, IdType>) -> Self::Future {
        let tracker = TaskTracker::new();
        task.parts.data.insert(tracker.clone());
        let worker: WorkerContext = task.parts.data.get().cloned().unwrap();
        let req = self.service.call(task);
        async move {
            let res = req.await;
            let _ = tracker.close();
            let tracker_fut = worker.track(tracker.wait()); // Long running tasks will be awaited in a shutdown
            tracker_fut.await;
            res
        }
        .boxed()
    }
}

/// Helper trait for building long running workers from [`WorkerBuilder`]
///
/// See [module level documentation](self) for more details.
pub trait LongRunningExt<Args, Ctx, Source, Middleware>: Sized {
    /// Extension for executing long running jobs
    fn long_running(self) -> WorkerBuilder<Args, Ctx, Source, Stack<LongRunningLayer, Middleware>> {
        self.long_running_with_cfg(Default::default())
    }
    /// Extension for executing long running jobs with a config
    fn long_running_with_cfg(
        self,
        cfg: LongRunningConfig,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<LongRunningLayer, Middleware>>;
}

impl<Args, B, M, Ctx> LongRunningExt<Args, Ctx, B, M> for WorkerBuilder<Args, Ctx, B, M>
where
    M: Layer<LongRunningLayer>,
    B: Backend<Args = Args, Context = Ctx>,
{
    fn long_running_with_cfg(
        self,
        cfg: LongRunningConfig,
    ) -> WorkerBuilder<Args, Ctx, B, Stack<LongRunningLayer, M>> {
        let this = self.layer(LongRunningLayer::new(cfg));
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{
        backend::{TaskSink, memory::MemoryStorage},
        error::BoxDynError,
        worker::{
            builder::WorkerBuilder,
            context::WorkerContext,
            ext::{event_listener::EventListenerExt, long_running::LongRunningExt},
        },
    };

    use super::*;

    const ITEMS: u32 = 1_000_000;

    #[tokio::test]
    async fn basic_worker() {
        let mut in_memory = MemoryStorage::new();
        for i in 0..ITEMS {
            in_memory.push(i).await.unwrap();
        }

        async fn task(
            task: u32,
            runner: LongRunnerCtx,
            worker: WorkerContext,
        ) -> Result<(), BoxDynError> {
            tokio::spawn(runner.track(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }));
            if task == ITEMS - 1 {
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    worker.stop().unwrap();
                });
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .long_running()
            .on_event(|ctx, ev| {
                println!("On Event = {ev:?} from {}", ctx.name());
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
