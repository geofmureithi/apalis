#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! Apalis is a simple, extensible multithreaded background job processing library for Rust.
//! ## Core Features
//! - Simple and predictable job handling model.
//! - Jobs handlers with a macro free API.
//! - Take full advantage of the [`tower`] ecosystem of
//!   middleware, services, and utilities.
//! - Takes full advantage of the actor model with each worker being an [`Actor`].
//! - Bring your own Storage.
//!
//! Apalis job processing is powered by [`tower::Service`] which means you have access to the [`tower`] and [`tower-http`] middleware.
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
//! [`tower::service`]: https://docs.rs/tower/latest/tower/trait.Service.html
//! [`tower`]: https://crates.io/crates/tower
//! [`tower-http`]: https://crates.io/crates/tower-http
//! [`Layer`]: https://docs.rs/tower/latest/tower/trait.Layer.html

/// Include the default Redis storage
///
/// ### Example
/// ```ignore
/// let storage = RedisStorage::connect(REDIS_URL).await
///                 .expect("Cannot establish connection");
///
/// let pubsub = RedisPubSubListener::new(storage.get_connection());
///
/// Monitor::new()
///     .register_with_count(4, move |_| {
///         WorkerBuilder::new(storage.clone())
///             .build_fn(email_service)
///             .start()
///     })
///     .event_handler(pubsub)
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

/// Apalis jobs fully support tower middleware via [`Layer`]
///
/// ## Example
/// ```ignore
/// use apalis::{
///     layers::{Extension, DefaultRetryPolicy, RetryLayer},
///     WorkerBuilder,
/// };
///
/// fn main() {
///     let queue = WorkerBuilder::new(storage)
///         .layer(RetryLayer::new(DefaultRetryPolicy))
///         .layer(Extension(GlobalState {}))
///         .build();
///     let addr = queue.start();
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
    pub use apalis_core::{
        builder::WorkerBuilder,
        builder::WorkerFactory,
        builder::WorkerFactoryFn,
        context::JobContext,
        error::JobError,
        job::{Counts, Job, JobFuture, JobStreamExt},
        job_fn::job_fn,
        monitor::Monitor,
        request::JobRequest,
        request::JobState,
        response::IntoResponse,
        storage::builder::WithStorage,
        storage::Storage,
        storage::StorageWorkerPulse,
    };
}
