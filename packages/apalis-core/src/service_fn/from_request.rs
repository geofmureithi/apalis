//! Offers dependency injection for service functions
//!
//! Items have this already implemented:
//! - [`Attempt`]
//! - [`WorkerContext`]
//! - [`Data<T>`]
//! - [`TaskId`]
//! 
//! # Example
//! 
//! Say we have a basic task for sending emails given the user id
//! ```rust
//! struct Email {
//!     user_id: String,
//!     subject: String,
//!     message: String
//! }
//! async fn send_email_by_id(email: Email) -> Result<(), BoxDynError> {
//!     let user_id = email.user_id;
//!     let user = User::find_by_id(user_id).await?;
//!     // Do something with user
//! }
//! ```
//! 
//! With [`FromRequest`] you can improve the experience by:
//! ```rust
//! impl <Ctx> FromRequest<Request<Email, Ctx>> for User {
//!     type Error = BoxDynError;
//!     async fn from_request(req: Request<Email, Ctx>) -> Result<Self, BoxDynError> {
//!         let user_id = req.args.user_id;
//!         let user = User::find_by_id(user_id).await?;
//!         Ok(user)
//!     }
//! }
//! 
//! async fn send_email(email: Email, user: User) -> Result<(), BoxDynError> {
//!     // Do something with user
//! }
//! ```
//! [`FromRequest`]: crate::service_fn::from_request::FromRequest
//! [`Attempt`]: crate::request::attempt::Attempt
//! [`Data<T>`]: crate::request::data::Data
//! [`WorkerContext`]: crate::worker::context::WorkerContext
//! [`TaskId`]: crate::request::task_id::TaskId

use std::future::Future;

pub trait FromRequest<Req>: Sized {
    type Error;
    /// Perform the extraction.
    fn from_request(req: &Req) -> impl Future<Output = Result<Self, Self::Error>> + Send;
}
