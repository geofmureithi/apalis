#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! # Apalis Sql Storage
//! Apalis offeres Sqlite, Mysql and Postgres storages for its workers.
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
//!      async fn send_email(job: Email, _ctx: JobContext) -> Result<JobResult, JobError> {
//!          log::info!("Attempting to send email to {}", job.to);
//!          Ok(JobResult::Success)
//!      }
//!     // This can be even in another program/language
//!     let query = "Select apalis.push_job('apalis::Email', json_build_object('subject', 'Test Apalis', 'to', 'test1@example.com', 'text', 'Lorem Ipsum'));";
//!     db.execute(query).await.unwrap();
//!
//!      Monitor::new()
//!          .register_with_count(4, move |_| {
//!              WorkerBuilder::new(pg.clone())
//!                  .build_fn(send_email)
//!          })
//!          .run()
//!          .await
//!  }
//! ```

mod from_row;

/// Postgres storage for Apalis. Uses `NOTIFY` and `SKIP LOCKED`
#[cfg(feature = "postgres")]
#[cfg_attr(docsrs, doc(cfg(feature = "postgres")))]
pub mod postgres;

/// Sqlite Storage for Apalis.
/// Uses a transaction and min(rowid)
#[cfg(feature = "sqlite")]
#[cfg_attr(docsrs, doc(cfg(feature = "sqlite")))]
pub mod sqlite;

/// MySQL storage for Apalis. Uses `SKIP LOCKED`
#[cfg(feature = "mysql")]
#[cfg_attr(docsrs, doc(cfg(feature = "mysql")))]
pub mod mysql;
