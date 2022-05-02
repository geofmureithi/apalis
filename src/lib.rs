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
