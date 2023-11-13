#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! apalis is a simple, extensible multithreaded background job processing library for rust.
//! ## Core Features
//! - Simple and predictable functional job handling model with a macro free API.
//! - Takes full advantage of the [`tower`] ecosystem of
//!   middleware, services, and utilities.
//! - Anything that implements [`Stream`] can be used as a job source.
//! - Runtime agnostic with inbuilt support for tokio and async-std.
//! - Provides high concurrency, and allows for configuration of workers, jobs and thread pool.
//!
//! An apalis job is powered by a tower [`Service`] which means you have access to the [`tower`] middleware.
//!  ### Example
//! ```rust, no_run
//! use apalis::prelude::*;
//! use serde::{Deserialize, Serialize};
//! use apalis_redis::RedisStorage;
//!
//! #[derive(Debug, Deserialize, Serialize)]
//! struct Email {
//!     to: String,
//! }
//!
//! impl Job for Email {
//!     const NAME: &'static str = "apalis::Email";
//! }
//!
//! async fn send_email(job: Email, ctx: JobContext) -> Result<(), JobError> {
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let redis = std::env::var("REDIS_URL").expect("Missing REDIS_URL env variable");
//!     let storage = RedisStorage::connect(redis).await.expect("Storage failed");
//!     Monitor::new()
//!         .register_with_count(2, move |index| {
//!             WorkerBuilder::new(&format!("quick-sand-{index}"))
//!                 .with_storage(storage.clone())
//!                 .build_fn(send_email)
//!         })
//!         .run()
//!         .await
//!         .unwrap();
//! }
//!```
//!
//! ## Web UI Available
//! ![UI](https://github.com/geofmureithi/apalis-board/raw/master/screenshots/workers.png)
//! See [this example](https://github.com/geofmureithi/apalis/tree/master/examples/rest-api)
//! ## Feature flags
#![cfg_attr(
    feature = "docsrs",
    cfg_attr(doc, doc = ::document_features::document_features!())
)]
//!
//! [`Service`]: https://docs.rs/tower/latest/tower/trait.Service.html
//! [`tower`]: https://crates.io/crates/tower
//! [`tower-http`]: https://crates.io/crates/tower-http
//! [`Layer`]: https://docs.rs/tower/latest/tower/trait.Layer.html
//! [`Stream`]: https://docs.rs/futures/latest/futures/stream/trait.Stream.html

/// Include the default Redis storage
///
/// ### Example
/// ```ignore
/// let storage = RedisStorage::connect("REDIS_URL").await
///                 .expect("Cannot establish connection");
///
/// Monitor::new()
///     .register_with_count(4, move |index| {
///         WorkerBuilder::new(&format!("quick-sand-{index}"))
///             .with_storage(storage.clone())
///             .build_fn(email_service)
///     })
///     .run()
///     .await
///```
#[cfg(feature = "redis")]
#[cfg_attr(docsrs, doc(cfg(feature = "redis")))]
pub mod redis {
    pub use apalis_redis::RedisStorage;
}

/// Include the default Sqlite storage
#[cfg(feature = "sqlite")]
#[cfg_attr(docsrs, doc(cfg(feature = "sqlite")))]
pub mod sqlite {
    pub use apalis_sql::sqlite::*;
}

/// Include the default Postgres storage
#[cfg(feature = "postgres")]
#[cfg_attr(docsrs, doc(cfg(feature = "postgres")))]
pub mod postgres {
    pub use apalis_sql::postgres::*;
}

/// Include the default MySQL storage
#[cfg(feature = "mysql")]
#[cfg_attr(docsrs, doc(cfg(feature = "mysql")))]
pub mod mysql {
    pub use apalis_sql::mysql::*;
}

/// Include Cron utilities
#[cfg(feature = "cron")]
#[cfg_attr(docsrs, doc(cfg(feature = "cron")))]
pub mod cron {
    pub use apalis_cron::*;
}

/// apalis jobs fully support tower middleware via [`Layer`]
///
/// ## Example
/// ```ignore
/// use apalis::{
///     layers::{Extension, DefaultRetryPolicy, RetryLayer},
///     WorkerBuilder,
/// };
///
/// fn main() {
///     let worker = WorkerBuilder::new("tasty-avocado")
///         .layer(RetryLayer::new(DefaultRetryPolicy))
///         .layer(Extension(GlobalState {}))
///         .with_storage(storage.clone())
///         .build(send_email);
/// }

/// ```
///
/// [`Layer`]: https://docs.rs/tower/latest/tower/trait.Layer.html
pub mod layers {
    #[cfg(feature = "retry")]
    #[cfg_attr(docsrs, doc(cfg(feature = "retry")))]
    pub use apalis_core::layers::retry::RetryLayer;

    #[cfg(feature = "retry")]
    #[cfg_attr(docsrs, doc(cfg(feature = "retry")))]
    pub use apalis_core::layers::retry::DefaultRetryPolicy;

    #[cfg(feature = "tracing")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tracing")))]
    pub use apalis_core::layers::tracing::{Trace, TraceLayer};

    #[cfg(feature = "limit")]
    #[cfg_attr(docsrs, doc(cfg(feature = "limit")))]
    pub use apalis_core::layers::limit::RateLimitLayer;

    #[cfg(feature = "extensions")]
    #[cfg_attr(docsrs, doc(cfg(feature = "extensions")))]
    pub use apalis_core::layers::extensions::Extension;

    #[cfg(feature = "sentry")]
    #[cfg_attr(docsrs, doc(cfg(feature = "sentry")))]
    pub use apalis_core::layers::sentry::SentryJobLayer;

    #[cfg(feature = "prometheus")]
    #[cfg_attr(docsrs, doc(cfg(feature = "prometheus")))]
    pub use apalis_core::layers::prometheus::PrometheusLayer;
}

/// Common imports
pub mod prelude {
    #[cfg(feature = "expose")]
    #[cfg_attr(docsrs, doc(cfg(feature = "expose")))]
    pub use apalis_core::expose::*;
    pub use apalis_core::{
        builder::WorkerBuilder,
        builder::WorkerFactory,
        builder::WorkerFactoryFn,
        context::JobContext,
        error::JobError,
        executor::*,
        job::{Job, JobFuture, JobId},
        job_fn::job_fn,
        monitor::Monitor,
        request::JobRequest,
        request::JobState,
        response::IntoResponse,
        utils::*,
        worker::{WorkerContext, WorkerId},
    };
    pub use apalis_core::{
        storage::builder::WithStorage, storage::Storage, storage::StorageWorkerPulse,
    };
}
