//! # Creating task handlers
//!
//! This tutorial covers the basics of writing task handlers,
//! and implementing custom argument extraction via [`FromRequest`].
//!
//! ## 1. Writing basic task handlers
//!
//! Task handlers are async functions that process a task. You can use the [`task_fn`] helper
//! to wrap your handler into a service.
//!
//! ```rust
//! # use apalis_core::task::Data;
//! #[derive(Clone)]
//! struct State;
//!
//! // A simple handler that takes an id and injected state
//! async fn handler(id: u32, state: Data<State>) -> String {
//!     format!("Got id {} with state", id)
//! }
//! ```
//! You would need to inject the state in your worker builder:
//!
//! ```rs
//! let worker = WorkerBuilder::new()
//!     .backend(in_memory)
//!     .data(State)
//!     .build(handler);
//! ```
//!
//! ## 2. Dependency Injection in handlers
//!
//! `apalis-core` supports default injection for common types in your handler arguments, such as:
//! - [`Attempt`]: Information about the current attempt
//! - [`WorkerContext`]: Worker context
//! - [`Data<T>`]: Injected data/state
//! - [`TaskId`]: The unique ID of the task
//!
//! Example:
//! ```rust
//! use apalis_core::task::{Attempt, Data, TaskId};
//!
//! #[derive(Clone)]
//! struct State;
//!
//! async fn process_task(_: Email, attempt: Attempt, state: Data<State>, id: TaskId) -> String {
//!     format!("Attempt {} for task {} with state", attempt.count(), id)
//! }
//! ```
//!
//!
//! ## 3. Implementing custom argument extraction with [`FromRequest`]
//!
//! You can extract custom types from the request by implementing [`FromRequest`].
//!
//! Suppose you have a task to send emails, and you want to automatically extract a `User` from the task's `user_id`:
//!
//! ```rust
//! struct Email {
//!     user_id: String,
//!     subject: String,
//!     message: String,
//! }
//!
//! struct User {
//!     id: String,
//!     // other fields...
//! }
//!
//! // Implement FromRequest for User
//! # use apalis_core::task_fn::from_request::FromRequest;
//! # use apalis_core::task::Task;
//!
//! impl<Ctx> FromRequest<Task<Email, Ctx>> for User {
//!     type Error = BoxDynError;
//!     async fn from_request(req: Task<Email, Ctx>) -> Result<Self, Self::Error> {
//!         let user_id = req.args.user_id.clone();
//!         // Simulate fetching user from DB
//!         Ok(User { id: user_id })
//!     }
//! }
//!
//! // Now your handler can take User directly
//! async fn send_email(email: Email, user: User) -> Result<(), BoxDynError> {
//!     // Use email and user
//!     Ok(())
//! }
//! ```
//!
//! ## 4. How It Works
//!
//! - [`task_fn`] wraps your handler into a [`TaskFn`] service.
//! - Arguments are extracted using [`FromRequest`].
//! - DI types are injected automatically.
//! - The handler's output is converted to a response using [`IntoResponse`].
//!
//! [`task_fn`]: crate::task_fn::task_fn
//! [`TaskFn`]: crate::task_fn::TaskFn
//! [`FromRequest`]: crate::task_fn::from_request::FromRequest
//! [`IntoResponse`]: crate::task_fn::into_response::IntoResponse
//! [`Attempt`]: crate::task::attempt::Attempt
//! [`Data<T>`]: crate::task::data::Data
//! [`WorkerContext`]: crate::worker::context::WorkerContext
//! [`TaskId`]: crate::task::task_id::TaskId
