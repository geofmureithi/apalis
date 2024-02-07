#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! apalis storage using Redis as a backend
//! ```rust,no_run
//! use email_service::send_email;
//! # use apalis_core::monitor::Monitor;
//! # use apalis_redis::RedisStorage;
//! # use apalis_core::builder::WorkerBuilder;
//! # use apalis_core::builder::WorkerFactoryFn;
//! # use apalis_utils::TokioExecutor;
//! #[tokio::main]
//! async fn main() -> std::io::Result<()> {
//!     let storage = RedisStorage::connect("redis://localhost").await.unwrap();
//!     Monitor::<TokioExecutor>::new()
//!        .register(
//!            WorkerBuilder::new("tasty-pear")
//!                .source(storage.clone())
//!                .build_fn(send_email),
//!        )
//!        .run()
//!        .await
//! }
//! ```

mod storage;
pub use storage::RedisStorage;
pub use storage::connect;
