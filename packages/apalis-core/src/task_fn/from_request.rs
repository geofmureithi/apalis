//! Traits for offering dependency injection in task functions.
//!
//! This allows for more ergonomic access to common types within the task context.
//! Items have this already implemented:
//! - [`Attempt`]
//! - [`WorkerContext`]
//! - [`Data<T>`] where T is injected via `task.data(...)`
//! - [`TaskId`]
//!
//! # Example
//!
//! Say we have a basic task for sending emails given the user id
//! ```rust
//! # use apalis_core::error::BoxDynError;
//! # struct User {
//! #     id: String,
//! #     // other fields...
//! # }
//! # impl User {
//! #     async fn find_by_id(id: String) -> Result<Self, BoxDynError> {
//! #         // Simulate fetching user from DB
//! #         Ok(User { id })
//! #     }
//! # }
//! struct Email {
//!     user_id: String,
//!     subject: String,
//!     message: String
//! }
//! async fn send_email_by_id(email: Email) -> Result<(), BoxDynError> {
//!     let user_id = email.user_id;
//!     let user = User::find_by_id(user_id).await?;
//!     // Do something with user
//! 
//!     Ok(())
//! }
//! ```
//!
//! With [`FromRequest`] you can improve the experience by:
//! ```rust
//! # use apalis_core::task::Task;
//! # use apalis_core::error::BoxDynError;
//! # use apalis_core::task_fn::FromRequest;
//! # struct User {
//! #     id: String,
//! #     // other fields...
//! # }
//! # impl User {
//! #     async fn find_by_id(id: &String) -> Result<Self, BoxDynError> {
//! #         // Simulate fetching user from DB
//! #         Ok(User { id: id.clone() })
//! #     }
//! # }
//! # struct Email { user_id: String };
//! impl <Ctx: Sync> FromRequest<Task<Email, Ctx>> for User {
//!     type Error = BoxDynError;
//!     async fn from_request(req: &Task<Email, Ctx>) -> Result<Self, BoxDynError> {
//!         let user_id = &req.args.user_id;
//!         let user = User::find_by_id(user_id).await?;
//!         Ok(user)
//!     }
//! }
//!
//! async fn send_email(email: Email, user: User) -> Result<(), BoxDynError> {
//!     // Do something with user
//!     Ok(())
//! }
//! ```
//! [`FromRequest`]: crate::task_fn::FromRequest
//! [`Attempt`]: crate::task::attempt::Attempt
//! [`Data<T>`]: crate::task::data::Data
//! [`WorkerContext`]: crate::worker::context::WorkerContext
//! [`TaskId`]: crate::task::task_id::TaskId

use std::future::Future;

/// A trait for extracting types from a task's context.
pub trait FromRequest<Task>: Sized {
    /// The error type that can occur during extraction.
    type Error;
    /// Perform the extraction.
    fn from_request(req: &Task) -> impl Future<Output = Result<Self, Self::Error>> + Send;
}
