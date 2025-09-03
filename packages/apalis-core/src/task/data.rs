
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
//! Use [`Data`] to share application state (such as database connections, configuration, etc.) across tasks and layers. The [`Layer`] implementation allows easy integration with middleware stacks.
//!
//! ## Example
//!
//! ```rust
//! use std::sync::Arc;
//! use apalis_core::layers::extensions::Data;
//! use apalis_core::service_fn::service_fn;
//! use apalis_core::builder::WorkerBuilder;
//! use apalis_core::memory::MemoryStorage;
//!
//! struct State { /* ... */ }
//! struct Email;
//!
//! async fn email_service(email: Email, state: Data<Arc<State>>) {
//!     // Use shared state here
//! }
//!
//! let state = Arc::new(State { /* ... */ });
//! let worker = WorkerBuilder::new("tasty-avocado")
//!     .data(state)
//!     .backend(MemoryStorage::new())
//!     .build(service_fn(email_service));
//! ```
//!
//! # Features
//!
//! - Type-safe access to shared data.
//! - Middleware integration via [`tower_layer::Layer`].
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

use crate::{task_fn::FromRequest, task::Task};

/// Extension data for tasks.
/// This is commonly used to share state across tasks. or across layers within the same tasks
///
/// ```rust
/// # use std::sync::Arc;
/// # struct Email;
/// # use apalis_core::layers::extensions::Data;
/// # use apalis_core::service_fn::service_fn;
/// # use crate::apalis_core::builder::WorkerFactory;
/// # use apalis_core::builder::WorkerBuilder;
/// # use apalis_core::memory::MemoryStorage;
/// // Some shared state used throughout our application
/// struct State {
///     // ...
/// }
///
/// async fn email_service(email: Email, state: Data<Arc<State>>) {
///     
/// }
///
/// let state = Arc::new(State { /* ... */ });
///
/// let worker = WorkerBuilder::new("tasty-avocado")
///     .data(state)
///     .backend(MemoryStorage::new())
///     .build(service_fn(email_service));
/// ```

#[derive(Debug, Clone, Copy)]
pub struct Data<T>(T);
impl<T> Data<T> {
    /// Build a new data entry
    pub fn new(inner: T) -> Data<T> {
        Data(inner)
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

impl<S, T, Args, Meta, IdType> Service<Task<Args, Meta, IdType>> for AddExtension<S, T>
where
    S: Service<Task<Args, Meta, IdType>>,
    T: Clone + Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Task<Args, Meta, IdType>) -> Self::Future {
        req.ctx.data.insert(self.value.clone());
        self.inner.call(req)
    }
}

/// Error type for missing data in a task's context.
#[derive(Debug, thiserror::Error)]
pub enum MissingDataError {
    /// The type was not found in the task's data map
    #[error("the type for key `{0}` is not available")]
    NotFound(String),
}

impl<T: Clone + Send + Sync + 'static, Args: Sync, Meta: Sync, IdType: Sync + Send> FromRequest<Task<Args, Meta, IdType>>
    for Data<T>
{
    type Error = MissingDataError;
    async fn from_request(req: &Task<Args, Meta, IdType>) -> Result<Self, Self::Error> {
        req.ctx.data.get_checked().cloned().map(Data::new)
    }
}
