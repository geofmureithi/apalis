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
//! use apalis_redis::{RedisStorage, Config};
//!
//! #[derive(Debug, Deserialize, Serialize)]
//! struct Email {
//!     to: String,
//! }
//!
//! async fn send_email(job: Email, data: Data<usize>) -> Result<(), Error> {
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let redis = std::env::var("REDIS_URL").expect("Missing REDIS_URL env variable");
//!     let conn = apalis_redis::connect(redis).await.unwrap();
//!     let storage = RedisStorage::new(conn);
//!     Monitor::new()
//!         .register({
//!             WorkerBuilder::new(&format!("quick-sand"))
//!                 .concurrency(2)
//!                 .data(0usize)
//!                 .backend(storage.clone())
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
//! See [this example](https://github.com/geofmureithi/apalis/tree/main/examples/rest-api)
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
/// apalis fully supports middleware via [`Layer`](https://docs.rs/tower/latest/tower/trait.Layer.html)
pub mod layers;

/// Common imports
pub mod prelude {
    pub use crate::layers::WorkerBuilderExt;
    pub use apalis_core::{
        backend::Backend,
        backend::BackendExpose,
        backend::Stat,
        backend::WorkerState,
        builder::{WorkerBuilder, WorkerFactory, WorkerFactoryFn},
        codec::Codec,
        data::Extensions,
        error::{BoxDynError, Error},
        layers::extensions::{AddExtension, Data},
        memory::{MemoryStorage, MemoryWrapper},
        monitor::Monitor,
        mq::MessageQueue,
        notify::Notify,
        poller::stream::BackendStream,
        poller::{controller::Controller, Poller},
        request::State,
        request::{Request, RequestStream},
        response::IntoResponse,
        service_fn::{service_fn, FromRequest, ServiceFn},
        step::*,
        storage::Storage,
        task::attempt::Attempt,
        task::task_id::TaskId,
        worker::{Context, Event, Ready, Worker, WorkerError, WorkerId},
    };
}
