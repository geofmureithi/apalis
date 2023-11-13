#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! apalis storage using Redis as a backend
//! ```rust,ignore
//! #[tokio::main]
//! async fn main() -> std::io::Result<()> {
//!     let storage = RedisStorage::connect("REDIS_URL").await.unwrap();
//!     Monitor::new()
//!        .register(
//!            WorkerBuilder::new("tasty-pear")
//!                .with_storage(storage.clone())
//!                .build_fn(send_email),
//!        )
//!        .run()
//!        .await
//! }
//! ```

mod storage;
pub use storage::RedisStorage;

#[cfg(feature = "chrono")]
type Timestamp = chrono::DateTime<chrono::Utc>;
#[cfg(all(not(feature = "chrono"), feature = "time"))]
type Timestamp = time::OffsetDateTime;
