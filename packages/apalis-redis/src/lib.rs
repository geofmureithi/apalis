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

#[cfg(feature = "pubsub")]
pub mod listener;
