#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! # apalis-sql
//! apalis offers Sqlite, Mysql and Postgres storages for its workers.
//!
//! ## Example
//!  ```rust,ignore
//!  #[tokio::main]
//!  async fn main() -> std::io::Result<()> {
//!      std::env::set_var("RUST_LOG", "debug,sqlx::query=error");
//!      let database_url = std::env::var("DATABASE_URL").expect("Must specify url to db");
//!
//!      let pg: PostgresStorage<Email> = PostgresStorage::connect(database_url).await.unwrap();
//!
//!      async fn send_email(job: Email, _ctx: JobContext) -> Result<(), JobError> {
//!          log::info!("Attempting to send email to {}", job.to);
//!          Ok(())
//!      }
//!     // This can be even in another program/language
//!     let query = "Select apalis.push_job('apalis::Email', json_build_object('subject', 'Test apalis', 'to', 'test1@example.com', 'text', 'Lorem Ipsum'));";
//!     db.execute(query).await.unwrap();
//!
//!      Monitor::new()
//!          .register_with_count(4, move |index| {
//!              WorkerBuilder::new(&format!("tasty-avocado-{index}"))
//!                  .with_storage(pg)
//!                  .build_fn(send_email)
//!          })
//!          .run()
//!          .await
//!  }
//! ```

mod from_row;

/// Postgres storage for apalis. Uses `NOTIFY` and `SKIP LOCKED`
#[cfg(feature = "postgres")]
#[cfg_attr(docsrs, doc(cfg(feature = "postgres")))]
pub mod postgres;

/// Sqlite Storage for apalis.
/// Uses a transaction and min(rowid)
#[cfg(feature = "sqlite")]
#[cfg_attr(docsrs, doc(cfg(feature = "sqlite")))]
pub mod sqlite;

/// MySQL storage for apalis. Uses `SKIP LOCKED`
#[cfg(feature = "mysql")]
#[cfg_attr(docsrs, doc(cfg(feature = "mysql")))]
pub mod mysql;

#[cfg(feature = "chrono")]
use chrono::{DateTime, Utc};
#[cfg(feature = "time")]
use time::OffsetDateTime;

#[cfg(feature = "chrono")]
#[allow(dead_code)]
type Timestamp = DateTime<Utc>;
#[cfg(feature = "time")]
#[allow(dead_code)]
type Timestamp = OffsetDateTime;
