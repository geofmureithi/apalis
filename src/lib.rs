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
//! use apalis::redis::RedisStorage;
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
//! async fn send_email(job: Email, data: Data<usize>) -> Result<(), Error> {
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let redis = std::env::var("REDIS_URL").expect("Missing REDIS_URL env variable");
//!     let conn = apalis::redis::connect(redis).await.unwrap();
//!     let storage = RedisStorage::new(conn);
//!     Monitor::<TokioExecutor>::new()
//!         .register_with_count(2, {
//!             WorkerBuilder::new(&format!("quick-sand"))
//!                 .data(0usize)
//!                 .source(storage.clone())
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
#[cfg(feature = "redis")]
#[cfg_attr(docsrs, doc(cfg(feature = "redis")))]
pub mod redis {
    pub use apalis_redis::connect;
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

/// apalis fully supports middleware via [`Layer`](https://docs.rs/tower/latest/tower/trait.Layer.html)
pub mod layers;

/// Utilities for working with apalis
pub mod utils {
    /// Executor for [`tokio`]
    #[cfg(feature = "tokio-comp")]
    #[derive(Clone, Debug, Default)]
    pub struct TokioExecutor;

    #[cfg(feature = "tokio-comp")]
    impl apalis_core::executor::Executor for TokioExecutor {
        fn spawn(&self, future: impl std::future::Future<Output = ()> + Send + 'static) {
            tokio::spawn(future);
        }
    }

    /// Executor for [`async_std`]
    #[cfg(feature = "async-std-comp")]
    #[derive(Clone, Debug, Default)]
    pub struct AsyncStdExecutor;

    #[cfg(feature = "async-std-comp")]
    impl apalis_core::executor::Executor for AsyncStdExecutor {
        fn spawn(&self, future: impl std::future::Future<Output = ()> + Send + 'static) {
            async_std::task::spawn(future);
        }
    }
}

/// Common imports
pub mod prelude {
    #[cfg(feature = "tokio-comp")]
    pub use crate::utils::TokioExecutor;
    pub use apalis_core::{
        builder::{WorkerBuilder, WorkerFactory, WorkerFactoryFn},
        data::Extensions,
        error::{BoxDynError, Error},
        executor::Executor,
        layers::extensions::{AddExtension, Data},
        memory::{MemoryStorage, MemoryWrapper},
        monitor::{Monitor, MonitorContext},
        mq::{Message, MessageQueue},
        notify::Notify,
        poller::stream::BackendStream,
        poller::{controller::Controller, FetchNext, Poller},
        request::{Request, RequestStream},
        response::IntoResponse,
        service_fn::{service_fn, FromData, ServiceFn},
        storage::{Job, Storage, StorageStream},
        task::attempt::Attempt,
        task::task_id::TaskId,
        worker::{Context, Event, Ready, Worker, WorkerError, WorkerId},
        Backend, Codec,
    };
}
