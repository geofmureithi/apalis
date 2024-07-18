#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! # apalis-sql
//! apalis offers Sqlite, Mysql and Postgres storages for its workers.
//! See relevant modules for examples

use std::time::Duration;

use context::State;

/// The context of the sql job
pub mod context;
/// Util for fetching rows
pub mod from_row;

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

/// Config for sql storages
#[derive(Debug, Clone)]
pub struct Config {
    keep_alive: Duration,
    buffer_size: usize,
    poll_interval: Duration,
    namespace: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            keep_alive: Duration::from_secs(30),
            buffer_size: 10,
            poll_interval: Duration::from_millis(50),
            namespace: String::from("apalis::sql"),
        }
    }
}

impl Config {
    /// Create a new config with a jobs namespace
    pub fn new(namespace: &str) -> Self {
        Config::default().set_namespace(namespace)
    }

    /// Interval between database poll queries
    ///
    /// Defaults to 30ms
    pub fn set_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Interval between worker keep-alive database updates
    ///
    /// Defaults to 30s
    pub fn set_keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    /// Buffer size to use when querying for jobs
    ///
    /// Defaults to 10
    pub fn set_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Set the namespace to consume and push jobs to
    ///
    /// Defaults to "apalis::sql"
    pub fn set_namespace(mut self, namespace: &str) -> Self {
        self.namespace = namespace.to_owned();
        self
    }

    /// Gets a reference to the keep_alive duration.
    pub fn keep_alive(&self) -> &Duration {
        &self.keep_alive
    }

    /// Gets a mutable reference to the keep_alive duration.
    pub fn keep_alive_mut(&mut self) -> &mut Duration {
        &mut self.keep_alive
    }

    /// Gets the buffer size.
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Gets a reference to the poll_interval duration.
    pub fn poll_interval(&self) -> &Duration {
        &self.poll_interval
    }

    /// Gets a mutable reference to the poll_interval duration.
    pub fn poll_interval_mut(&mut self) -> &mut Duration {
        &mut self.poll_interval
    }

    /// Gets a reference to the namespace.
    pub fn namespace(&self) -> &String {
        &self.namespace
    }

    /// Gets a mutable reference to the namespace.
    pub fn namespace_mut(&mut self) -> &mut String {
        &mut self.namespace
    }
}

/// Calculates the status from a result
pub(crate) fn calculate_status(res: &Result<String, String>) -> State {
    match res {
        Ok(_) => State::Done,
        Err(e) => match &e {
            _ if e.starts_with("AbortError") => State::Killed,
            _ => State::Failed,
        },
    }
}

///
#[macro_export]
macro_rules! sql_storage_tests {
    ($setup:path, $storage_type:ty, $job_type:ty) => {
        async fn setup_test_wrapper() -> TestWrapper<$storage_type, $job_type> {
            let (t, poller) = TestWrapper::new_with_service(
                $setup().await,
                apalis_core::service_fn::service_fn(email_service::send_email),
            );
            tokio::spawn(poller);
            t
        }

        #[tokio::test]
        async fn integration_test_kill_job() {
            let mut storage = setup_test_wrapper().await;

            storage
                .push(email_service::example_killed_email())
                .await
                .unwrap();

            let (job_id, res) = storage.execute_next().await;
            assert_eq!(
                res,
                Err("AbortError: Invalid character. Job killed".to_owned())
            );
            let job = storage.fetch_by_id(&job_id).await.unwrap().unwrap();
            let ctx = job.get::<SqlContext>().unwrap();
            assert_eq!(*ctx.status(), State::Killed);
            assert!(ctx.done_at().is_some());
            assert_eq!(
                ctx.last_error().clone().unwrap(),
                "{\"Err\":\"AbortError: Invalid character. Job killed\"}"
            );
        }

        #[tokio::test]
        async fn integration_test_acknowledge_good_job() {
            let mut storage = setup_test_wrapper().await;
            storage
                .push(email_service::example_good_email())
                .await
                .unwrap();

            let (job_id, res) = storage.execute_next().await;
            assert_eq!(res, Ok("()".to_owned()));
            let job = storage.fetch_by_id(&job_id).await.unwrap().unwrap();
            let ctx = job.get::<SqlContext>().unwrap();
            assert_eq!(*ctx.status(), State::Done);
            assert!(ctx.done_at().is_some());
        }

        #[tokio::test]
        async fn integration_test_acknowledge_failed_job() {
            let mut storage = setup_test_wrapper().await;

            storage
                .push(email_service::example_retry_able_email())
                .await
                .unwrap();

            for index in 1..25 {
                let (job_id, res) = storage.execute_next().await;
                assert_eq!(
                    res,
                    Err("FailedError: Missing separator character '@'.".to_owned())
                );
                let job = storage.fetch_by_id(&job_id).await.unwrap().unwrap();
                let ctx = job.get::<SqlContext>().unwrap();
                assert_eq!(*ctx.status(), State::Failed);
                assert_eq!(ctx.attempts().current(), index);
                assert!(ctx.done_at().is_some());
                assert_eq!(
                    ctx.last_error().clone().unwrap(),
                    "{\"Err\":\"FailedError: Missing separator character '@'.\"}"
                );
            }
        }
    };
}
