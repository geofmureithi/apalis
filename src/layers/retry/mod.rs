//! Retry Policies for Apalis
//!
//! This module provides flexible retry strategies for tasks in Apalis workers.
//!
//! # Approaches
//!
//! ## 1. Fixed Retry Count
//!
//! Use [`RetryPolicy`] to retry a task a fixed number of times.
//!
//! ```rust
//! use apalis::layers::retry::RetryPolicy;
//!
//! let policy = RetryPolicy::retries(3);
//! ```
//!
//! ## 2. Retry with Backoff
//!
//! Use [`BackoffRetryPolicy`] to retry with a backoff strategy (e.g., exponential).
//!
//! ```rust
//! use apalis::layers::retry::{RetryPolicy, BackoffRetryPolicy};
//! use tower::retry::backoff::ExponentialBackoff;
//!
//! let backoff = ExponentialBackoff::from_millis(100);
//! let policy = RetryPolicy::retries(5).with_backoff(backoff);
//! ```
//!
//! ## 3. Conditional Retry
//!
//! Use [`RetryIfPolicy`] to retry only if a predicate matches the error.
//!
//! ```rust
//! use apalis::layers::retry::RetryPolicy;
//!
//! let policy = RetryPolicy::retries(3).retry_if(|err: &MyError| err.is_transient());
//! ```
//!
//! ## 4. Retry Based on Task Metadata
//!
//! Use [`WithTaskConfig`] to respect per-task retry configuration or else default to the underlying policy.
//!
//! ```rust
//! use apalis::layers::retry::{RetryPolicy, RetryConfig};
//!
//! let policy = RetryPolicy::default().with_task_config();
//! // Attach RetryConfig to your task's metadata
//! ```
//!
//! # Example: Worker with Retry Policy
//!
//! ```rust
//! use apalis::layers::retry::RetryPolicy;
//! use apalis::worker::builder::WorkerBuilder;
//!
//! let worker = WorkerBuilder::new("my-worker")
//!     .retry(RetryPolicy::retries(3))
//!     .build(my_task_fn);
//! ```
//!
//! See individual types for more details.

use apalis_core::error::AbortError;

use apalis_core::task::Task;
use apalis_core::task::builder::TaskBuilder;
use apalis_core::task::metadata::MetadataExt;
use std::any::Any;
use std::fmt::Debug;
use tower::retry::backoff::Backoff;

/// Re-exports from [`tower::retry`]
pub use tower::retry::*;
/// Re-exports from [`tower::util`]
pub use tower::util::rng::HasherRng;

/// Retries a task with backoff
#[derive(Clone, Debug)]
pub struct BackoffRetryPolicy<B> {
    retries: usize,
    backoff: B,
}

impl<B> BackoffRetryPolicy<B> {
    /// Build a new retry policy with backoff
    pub fn new(retries: usize, backoff: B) -> Self {
        Self { retries, backoff }
    }

    /// Retry the task if the predicate returns true
    pub fn retry_if<F, Err>(self, predicate: F) -> RetryIfPolicy<Self, F>
    where
        F: Fn(&Err) -> bool + Send + Sync + 'static,
    {
        RetryIfPolicy::new(self, predicate)
    }

    /// Retry the task based on [`RetryConfig`] metadata if exists
    ///
    /// Falls back to the [`RetryPolicy`] if no metadata is found.
    pub fn from_task_config(self) -> FromTaskConfigPolicy<Self> {
        FromTaskConfigPolicy::new(self)
    }
}

impl<T, Res, Ctx, B, Err: Any, IdType> Policy<Task<T, Ctx, IdType>, Res, Err>
    for BackoffRetryPolicy<B>
where
    T: Clone,
    Ctx: Clone,
    IdType: Clone,
    B: Backoff,
    B::Future: Send + 'static,
{
    type Future = B::Future;

    fn retry(
        &mut self,
        req: &mut Task<T, Ctx, IdType>,
        result: &mut Result<Res, Err>,
    ) -> Option<Self::Future> {
        let attempt = req.parts.attempt.current();
        match result.as_mut() {
            Ok(_) => {
                // Treat all `Response`s as success,
                // so don't retry...
                None
            }
            Err(_) if self.retries == 0 => {
                return None;
            }
            Err(err) if (err as &dyn Any).downcast_ref::<AbortError>().is_some() => {
                return None;
            }

            Err(_) if self.retries >= attempt => {
                return Some(self.backoff.next_backoff());
            }
            Err(_) => {
                return None;
            }
        }
    }

    fn clone_request(&mut self, req: &Task<T, Ctx, IdType>) -> Option<Task<T, Ctx, IdType>> {
        let req = req.clone();
        Some(req)
    }
}

/// Retries a task instantly for `retries`
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    retries: usize,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self { retries: 5 }
    }
}

impl RetryPolicy {
    /// Set the number of replies
    pub fn retries(retries: usize) -> Self {
        Self { retries }
    }

    /// Include a backoff to the retry policy
    pub fn with_backoff<B: Backoff>(self, backoff: B) -> BackoffRetryPolicy<B> {
        BackoffRetryPolicy {
            retries: self.retries,
            backoff,
        }
    }
    /// Retry the task if the predicate returns true
    pub fn retry_if<F, Err>(self, predicate: F) -> RetryIfPolicy<Self, F>
    where
        F: Fn(&Err) -> bool + Send + Sync + 'static,
    {
        RetryIfPolicy::new(self, predicate)
    }
    /// Retry the task based on [`RetryConfig`] metadata if exists
    ///
    /// Falls back to the [`RetryPolicy`] if no metadata is found.
    pub fn from_task_config(self) -> FromTaskConfigPolicy<Self> {
        FromTaskConfigPolicy::new(self)
    }
}

impl<T, Res, Ctx, Err: Any, IdType> Policy<Task<T, Ctx, IdType>, Res, Err> for RetryPolicy
where
    T: Clone,
    Ctx: Clone,
    IdType: Clone,
{
    type Future = std::future::Ready<()>;

    fn retry(
        &mut self,
        req: &mut Task<T, Ctx, IdType>,
        result: &mut Result<Res, Err>,
    ) -> Option<Self::Future> {
        let attempt = req.parts.attempt.current();
        match result.as_mut() {
            Ok(_) => {
                // Treat all `Response`s as success,
                // so don't retry...
                None
            }
            Err(_) if self.retries == 0 => {
                return None;
            }
            Err(err) if (err as &dyn Any).downcast_ref::<AbortError>().is_some() => {
                return None;
            }
            Err(_) if self.retries >= attempt => {
                return Some(std::future::ready(()));
            }
            Err(_) => {
                return None;
            }
        }
    }

    fn clone_request(&mut self, req: &Task<T, Ctx, IdType>) -> Option<Task<T, Ctx, IdType>> {
        let req = req.clone();
        Some(req)
    }
}

/// Retry the task if the predicate returns true
#[derive(Debug, Clone)]
pub struct RetryIfPolicy<P, F> {
    inner: P,
    predicate: F,
}

impl<P, F> RetryIfPolicy<P, F> {
    /// Build a new retry if policy
    pub fn new(inner: P, predicate: F) -> Self {
        Self { inner, predicate }
    }

    /// Retry the task based on [`RetryConfig`] metadata
    ///
    /// Falls back to the [`RetryIfPolicy`] if no metadata is found.
    pub fn from_task_config(self) -> FromTaskConfigPolicy<Self> {
        FromTaskConfigPolicy::new(self)
    }
}
impl<T, Res, Ctx, P, F, Err, IdType> Policy<Task<T, Ctx, IdType>, Res, Err> for RetryIfPolicy<P, F>
where
    T: Clone,
    Ctx: Clone,
    P: Policy<Task<T, Ctx, IdType>, Res, Err>,
    F: Fn(&Err) -> bool + Send + Sync + 'static,
{
    type Future = P::Future;

    fn retry(
        &mut self,
        req: &mut Task<T, Ctx, IdType>,
        result: &mut Result<Res, Err>,
    ) -> Option<Self::Future> {
        match result {
            Ok(_) => None,
            Err(err) => {
                if !(self.predicate)(err) {
                    return None;
                }
                self.inner.retry(req, result)
            }
        }
    }

    fn clone_request(&mut self, req: &Task<T, Ctx, IdType>) -> Option<Task<T, Ctx, IdType>> {
        self.inner.clone_request(req)
    }
}
/// Retry configuration for tasks
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// The maximum number of retries
    pub retries: usize,
}

/// Retry the task based on the [`RetryConfig`] metadata
#[derive(Debug, Clone)]
pub struct FromTaskConfigPolicy<P> {
    inner: P,
}

impl<P> FromTaskConfigPolicy<P> {
    /// Build a policy with metadata configuration
    pub fn new(inner: P) -> Self {
        Self { inner }
    }

    /// Retry the task if the predicate returns true
    pub fn retry_if<F, Err>(self, predicate: F) -> RetryIfPolicy<Self, F>
    where
        F: Fn(&Err) -> bool + Send + Sync + 'static,
    {
        RetryIfPolicy::new(self, predicate)
    }
}

impl Default for FromTaskConfigPolicy<RetryPolicy> {
    fn default() -> Self {
        Self {
            inner: RetryPolicy::retries(0),
        }
    }
}

impl<T, Res, Ctx, P, Err, IdType> Policy<Task<T, Ctx, IdType>, Res, Err> for FromTaskConfigPolicy<P>
where
    T: Clone,
    Ctx: Clone,
    P: Policy<Task<T, Ctx, IdType>, Res, Err>,
    Ctx: MetadataExt<RetryConfig>,
{
    type Future = P::Future;

    fn retry(
        &mut self,
        req: &mut Task<T, Ctx, IdType>,
        result: &mut Result<Res, Err>,
    ) -> Option<Self::Future> {
        match result {
            Ok(_) => None,
            Err(_) => {
                let attempt = req.parts.attempt.current();
                // If we have a retry config, we need to respect it
                if let Ok(cfg) = req.parts.ctx.extract() {
                    if cfg.retries <= attempt {
                        return None;
                    }
                };

                self.inner.retry(req, result)
            }
        }
    }

    fn clone_request(&mut self, req: &Task<T, Ctx, IdType>) -> Option<Task<T, Ctx, IdType>> {
        self.inner.clone_request(req)
    }
}

/// Retry metadata extension to include [`RetryConfig`]
pub trait RetryMetadataExt {
    /// Set number of retries
    fn retries(self, retries: usize) -> Self;
}

impl<Args, Ctx, IdType> RetryMetadataExt for TaskBuilder<Args, Ctx, IdType>
where
    Ctx: MetadataExt<RetryConfig>,
    Ctx::Error: Debug,
{
    /// Set number of retries in the metadata
    fn retries(self, retries: usize) -> Self {
        self.meta(RetryConfig { retries })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use apalis_core::{
        backend::memory::MemoryStorage,
        error::BoxDynError,
        task::{attempt::Attempt, builder::TaskBuilder},
        worker::{
            builder::WorkerBuilder, context::WorkerContext, ext::event_listener::EventListenerExt,
        },
    };
    use futures::SinkExt;

    use crate::layers::WorkerBuilderExt;

    use super::*;

    #[tokio::test]
    async fn basic_worker_retries() {
        let mut in_memory = MemoryStorage::new();

        let task1 = TaskBuilder::new(1).meta(RetryConfig { retries: 3 }).build();
        let task2 = TaskBuilder::new(2).retries(5).build();
        let task3 = TaskBuilder::new(3).build();

        in_memory.send(task1).await.unwrap();
        in_memory.send(task2).await.unwrap();
        in_memory.send(task3).await.unwrap();

        async fn task(
            task: u32,
            worker: WorkerContext,
            attempts: Attempt,
        ) -> Result<(), BoxDynError> {
            if task == 1 && attempts.current() == 4 {
                unreachable!("Task 1 reached 4 attempts");
            }
            if task == 3 && attempts.current() == 2 {
                unreachable!("Task 3 reached retried");
            }
            println!("Task {task} attempt {attempts:?}");
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task == 2 && attempts.current() == 4 {
                worker.stop().unwrap();
            }
            if task == 3 {
                return Err(SkipRetryError)?;
            }
            Err("Always fail if not 3")?
        }
        #[derive(Debug)]
        struct SkipRetryError;

        impl std::error::Error for SkipRetryError {}

        impl std::fmt::Display for SkipRetryError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "SkipRetryError")
            }
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .retry(
                RetryPolicy::retries(3)
                    // Use task config if it exists
                    .from_task_config()
                    // Skip retries for SkipRetryError
                    .retry_if(|e: &BoxDynError| e.downcast_ref::<SkipRetryError>().is_none()),
            )
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
