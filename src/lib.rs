//! Apalis is a simple, extensible multithreaded background job processing library for Rust.
//! ## Features
//! - Simple and predictable job handling model.
//! - Jobs handlers with a macro free API.
//! - Take full advantage of the [`tower`] ecosystem of
//!   middleware, services, and utilities.
//! - Takes full of the [`actix`] actors with each queue being an [`Actor`].
//! - Bring your own Storage.
//! 
//! Apalis job processing is powered by [`tower::Service`] which means you have access to the [`tower`] and [`tower-http`] middleware.
//!
//!  ## Example
//! ```no_run
//! use apalis::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Deserialize, Serialize)]
//! struct Email {
//!     to: String,
//! }
//!
//! async fn email_service(job: JobRequest<Email>) -> Result<JobResult, JobError> {
//!     Ok(JobResult::Success)
//! }
//!
//! #[actix_rt::main]
//! async fn main() -> std::io::Result<()> {
//!     let redis = std::env::var("REDIS_URL")
//!                     .expect("Missing env variable REDIS_URL");
//!     let storage = RedisStorage::new().await.unwrap();
//!     Worker::new()
//!         .register_with_count(2, move || {
//!             QueueBuilder::new(storage.clone())
//!                 .build_fn(email_service)
//!                 .start()
//!         })
//!         .run()
//!         .await
//! }
//! ```
//! [`tower::service`]: https://docs.rs/tower/latest/tower/trait.Service.html
//! [`tower`]: https://crates.io/crates/tower
//! [`actix`]: https://crates.io/crates/actix
//! [`tower-http`]: https://crates.io/crates/tower-http
//! [`actor`]: https://docs.rs/actix/0.13.0/actix/trait.Actor.html

pub use apalis_core::{
    builder::QueueBuilder,
    context::JobContext,
    error::JobError,
    job::{Job, JobFuture},
    queue::{Heartbeat, Queue},
    request::JobRequest,
    response::JobResult,
    storage::Storage,
    worker::Worker,
};

pub mod heartbeat {
    pub use apalis_core::streams::*;
}

#[cfg(feature = "redis")]
pub mod redis {
    pub use apalis_redis::RedisStorage;
}

//#[cfg(feature = "sqlite")]
pub mod sqlite {
    pub use apalis_sql::SqliteStorage;
}

/// Apalis jobs fully support tower middleware via [Layer]
///
/// ## Example
/// ```rust
/// use apalis::{
///     layers::{Extension, DefaultRetryPolicy, RetryLayer},
///     QueueBuilder,
/// };
///
/// fn main() {
///     let queue = QueueBuilder::new(storage.clone())
///         .layer(RetryLayer::new(DefaultRetryPolicy))
///         .layer(Extension(EmailState {}))
///         .build();
///     let addr = queue.start();
/// }

/// ```
///
/// [Layer]: https://docs.rs/tower/latest/tower/trait.Layer.html
pub mod layers {
    pub use apalis_core::layers::*;
}
