#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! apalis storage using Redis as a backend
//! ```rust,no_run
//! use apalis::prelude::*;
//! use apalis_redis::{RedisStorage, Config};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Deserialize, Serialize)]
//! struct Email {
//!     to: String,
//! }
//!
//! async fn send_email(job: Email) -> Result<(), Error> {
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let redis_url = std::env::var("REDIS_URL").expect("Missing env variable REDIS_URL");
//!     let conn = apalis_redis::connect(redis_url).await.expect("Could not connect");
//!     let storage = RedisStorage::new(conn);
//!     let worker = WorkerBuilder::new("tasty-pear")
//!         .backend(storage.clone())
//!         .build_fn(send_email);
//!
//!     worker.run().await;
//! }
//! ```

mod expose;
mod storage;
pub use redis::{aio::ConnectionManager, RedisError};
pub use storage::connect;
pub use storage::Config;
pub use storage::RedisContext;
pub use storage::RedisPollError;
pub use storage::RedisQueueInfo;
pub use storage::RedisStorage;
