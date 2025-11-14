//! Utilities for task runtime data extension.
//!
//! The [`Data`] type and related middleware are important for sharing state across tasks or layers within the same task.
//!
//! # Overview
//!
//! - [`Data<T>`]: Wraps a value of type `T` for sharing across tasks.
//! - [`AddExtension<S, T>`]: Middleware for injecting shared data into a task's context.
//! - [`MissingDataError`]: Error type for missing or unavailable data in a task context.
//!
//! # Usage
//!
//! Use [`Data`] to share application state (such as database connections, configuration, etc.) across tasks and layers. Apply this middleware using [`WorkerBuilder::data`](crate::worker::builder::WorkerBuilder::data).
//!
//! ## Example
//!
//! ```rust
//! # use std::sync::Arc;
//! # use apalis_core::task::data::Data;
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_core::backend::memory::MemoryStorage;
//!
//! struct State { /* ... */ }
//!
//! async fn email_service(email: String, state: Data<Arc<State>>) {
//!     // Use shared state here
//! }
//!
//! let state = Arc::new(State { /* ... */ });
//! let worker = WorkerBuilder::new("tasty-avocado")
//!     .backend(MemoryStorage::new())
//!     .data(state)
//!     .build(email_service);
//! ```
//!
//! # Features
//!
//! - Type-safe access to shared data.
//! - Integrated as middleware.
//! - Error handling for missing data.
//!
//! # See Also
//!
//! - [`FromRequest`] trait for extracting data from task contexts.
//! - [`Task`] type representing a unit of work.
use std::{
    ops::Deref,
    task::{Context, Poll},
};

use tower_service::Service;

use crate::{task::Task, task_fn::FromRequest};

/// Extension data for tasks.
/// This is commonly used to share state across tasks. or across layers within the same tasks
///
/// ```rust
/// # use std::sync::Arc;
/// # use apalis_core::task::data::Data;
/// # use apalis_core::worker::builder::WorkerBuilder;
/// # use apalis_core::backend::memory::MemoryStorage;
/// // Some shared state used throughout our application
/// struct State {
///     // ...
/// }
///
/// async fn send_email(email: String, state: Data<Arc<State>>) {
///     
/// }
///
/// let state = Arc::new(State { /* ... */ });
/// let backend = MemoryStorage::new();
///
/// let worker = WorkerBuilder::new("tasty-avocado")
///     .backend(backend)
///     .data(state)
///     .build(send_email);
/// ```

#[derive(Debug, Clone, Copy)]
pub struct Data<T>(T);
impl<T> Data<T> {
    /// Build a new data entry
    pub fn new(inner: T) -> Self {
        Self(inner)
    }
}

impl<T> Deref for Data<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S, T> tower_layer::Layer<S> for Data<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Service = AddExtension<S, T>;

    fn layer(&self, inner: S) -> Self::Service {
        AddExtension {
            inner,
            value: self.0.clone(),
        }
    }
}

/// Middleware for adding some shareable value to [request data].
#[derive(Clone, Copy, Debug)]
pub struct AddExtension<S, T> {
    inner: S,
    value: T,
}

impl<S, T, Args, Ctx, IdType> Service<Task<Args, Ctx, IdType>> for AddExtension<S, T>
where
    S: Service<Task<Args, Ctx, IdType>>,
    T: Clone + Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut task: Task<Args, Ctx, IdType>) -> Self::Future {
        task.parts.data.insert(self.value.clone());
        self.inner.call(task)
    }
}

/// Error type for missing data in a task's context.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum MissingDataError {
    /// The type was not found in the task's data map
    #[error("the type for key `{0}` is not available")]
    NotFound(String),
}

impl<T: Clone + Send + Sync + 'static, Args: Sync, Ctx: Sync, IdType: Sync + Send>
    FromRequest<Task<Args, Ctx, IdType>> for Data<T>
{
    type Error = MissingDataError;
    async fn from_request(task: &Task<Args, Ctx, IdType>) -> Result<Self, Self::Error> {
        task.parts.data.get_checked().cloned().map(Self::new)
    }
}
