use apalis_core::error::Error;
use apalis_core::poller::controller::Controller;
use apalis_core::request::Request;
use apalis_core::storage::{Job, Storage};
use apalis_core::worker::WorkerId;
use apalis_core::Codec;
use apalis_utils::codec::json::JsonCodec;
use apalis_utils::task_id::TaskId;
use async_stream::try_stream;
use futures::Stream;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::{MySql, MySqlPool, Pool, Row};
use std::convert::TryInto;
use std::sync::Arc;
use std::{marker::PhantomData, ops::Add, time::Duration};

use crate::from_row::SqlRequest;
use crate::Config;

/// Represents a [Storage] that persists to MySQL
#[derive(Debug)]
pub struct MysqlStorage<T> {
    pool: Pool<MySql>,
    job_type: PhantomData<T>,
    controller: Controller,
    config: Config,
    codec: Arc<Box<dyn Codec<T, String, Error = Error> + Sync + Send + 'static>>,
}

impl<T> Clone for MysqlStorage<T> {
    fn clone(&self) -> Self {
        let pool = self.pool.clone();
        MysqlStorage {
            pool,
            job_type: PhantomData,
            controller: self.controller.clone(),
            config: self.config.clone(),
            codec: self.codec.clone(),
        }
    }
}

impl<T> MysqlStorage<T> {
    /// Create a new instance from a pool
    pub fn new(pool: MySqlPool) -> Self {
        Self {
            pool,
            job_type: PhantomData,
            controller: Controller::new(),
            config: Config::default(),
            codec: Arc::new(Box::new(JsonCodec)),
        }
    }
    /// Create a Mysql Connection and start a Storage
    pub async fn connect<S: Into<String>>(db: S) -> Result<Self, sqlx::Error> {
        let pool = MySqlPool::connect(&db.into()).await?;
        Ok(Self::new(pool))
    }

    /// Get mysql migrations without running them
    #[cfg(feature = "migrate")]
    pub fn migrations() -> sqlx::migrate::Migrator {
        sqlx::migrate!("migrations/mysql")
    }

    /// Do migrations for mysql
    #[cfg(feature = "migrate")]
    pub async fn setup(&self) -> Result<(), sqlx::Error> {
        let pool = self.pool.clone();
        Self::migrations().run(&pool).await?;
        Ok(())
    }

    /// Expose the pool for other functionality, eg custom migrations
    pub fn pool(&self) -> &Pool<MySql> {
        &self.pool
    }
}

impl<T: DeserializeOwned + Send + Unpin + Job> MysqlStorage<T> {
    fn stream_jobs(
        &self,
        worker_id: &WorkerId,
        interval: Duration,
        buffer_size: usize,
    ) -> impl Stream<Item = Result<Option<Request<T>>, sqlx::Error>> {
        let pool = self.pool.clone();

        let worker_id = worker_id.clone();
        try_stream! {
            loop {
                apalis_utils::sleep(interval).await;
                let pool = pool.clone();
                // let mut tx = tx.acquire().await.map_err(|e| JobStreamError::BrokenPipe(Box::from(e)))?;
                let job_type = T::NAME;
                let fetch_query = "SELECT * FROM jobs
                WHERE status = 'Pending' AND run_at <= NOW() AND job_type = ? ORDER BY run_at ASC LIMIT ? FOR UPDATE";
                let jobs: Vec<SqlRequest<T>> = sqlx::query_as(fetch_query)
                    .bind(job_type)
                    .bind(i64::try_from(buffer_size).unwrap())
                    .fetch_all(&pool).await?;
                for job in jobs {
                    yield fetch_next(pool.clone(), &worker_id, job.build_job_request()).await?;
                }
            }
        }
    }

    async fn keep_alive_at<Service>(
        &mut self,
        worker_id: &WorkerId,
        last_seen: i64,
    ) -> Result<(), sqlx::Error> {
        let pool = self.pool.clone();

        let mut tx = pool.acquire().await?;
        let worker_type = T::NAME;
        let storage_name = std::any::type_name::<Self>();
        let query =
            "REPLACE INTO workers (id, worker_type, storage_name, layers, last_seen) VALUES (?, ?, ?, ?, ?);";
        sqlx::query(query)
            .bind(worker_id.to_string())
            .bind(worker_type)
            .bind(storage_name)
            .bind(std::any::type_name::<Service>())
            .bind(last_seen)
            .execute(&mut *tx)
            .await?;
        Ok(())
    }
}

async fn fetch_next<T>(
    pool: Pool<MySql>,
    worker_id: &WorkerId,
    job: Option<SqlRequest<T>>,
) -> Result<Option<SqlRequest<T>>, sqlx::Error>
where
    T: Send + Unpin + DeserializeOwned,
{
    match job {
        None => Ok(None),
        Some(job) => {
            let job_id = job.id();
            let update_query = "UPDATE jobs SET status = 'Running', lock_by = ?, lock_at = NOW() WHERE id = ? AND status = 'Pending' AND lock_by IS NULL;";
            sqlx::query(update_query)
                .bind(worker_id.to_string())
                .bind(job_id.to_string())
                .execute(&pool)
                .await?;
            let job: Option<SqlRequest<T>> = sqlx::query_as("Select * from jobs where id = ?")
                .bind(job_id.to_string())
                .fetch_optional(&pool)
                .await?;
            Ok(job)
        }
    }
}

impl<T> Storage for MysqlStorage<T>
where
    T: Job + Serialize + DeserializeOwned + Send + 'static + Unpin + Sync,
{
    type Job = T;

    type Error = sqlx::Error;

    type Identifier = TaskId;

    async fn push(&mut self, job: Self::Job) -> Result<TaskId, sqlx::Error> {
        let id = TaskId::new();
        let query =
            "INSERT INTO jobs VALUES (?, ?, ?, 'Pending', 0, 25, now(), NULL, NULL, NULL, NULL)";
        let pool = self.pool.clone();

        let job = serde_json::to_string(&job).unwrap();
        let job_type = T::NAME;
        sqlx::query(query)
            .bind(job)
            .bind(id.to_string())
            .bind(job_type.to_string())
            .execute(&pool)
            .await?;
        Ok(id)
    }

    async fn schedule(&mut self, job: Self::Job, on: i64) -> Result<TaskId, sqlx::Error> {
        let query =
            "INSERT INTO jobs VALUES (?, ?, ?, 'Pending', 0, 25, ?, NULL, NULL, NULL, NULL)";
        let pool = self.pool.clone();
        let id = TaskId::new();

        let job = serde_json::to_string(&job).map_err(|e| StorageError::Parse(e.into()))?;

        let job_type = T::NAME;
        sqlx::query(query)
            .bind(job)
            .bind(id.to_string())
            .bind(job_type)
            .bind(on)
            .execute(&pool)
            .await?;
        Ok(id)
    }

    async fn fetch_by_id(
        &self,
        job_id: &TaskId,
    ) -> Result<Option<Request<Self::Job>>, sqlx::Error> {
        let pool = self.pool.clone();
        let fetch_query = "SELECT * FROM jobs WHERE id = ?";
        let res: Option<SqlRequest<T>> = sqlx::query_as(fetch_query)
            .bind(job_id.to_string())
            .fetch_optional(&pool)
            .await?;
        Ok(res)
    }

    // async fn heartbeat(&mut self, pulse: StorageWorkerPulse) -> Result<bool, sqlx::Error> {
    //     let pool = self.pool.clone();

    //     match pulse {
    //         StorageWorkerPulse::EnqueueScheduled { count: _ } => {
    //             // Ideally jobs are queue via run_at. So this is not necessary
    //             Ok(true)
    //         }
    //         // Worker not seen in 5 minutes yet has running jobs
    //         StorageWorkerPulse::ReenqueueOrphaned {
    //             count,
    //             timeout_worker,
    //         } => {
    //             let job_type = T::NAME;
    //             let mut tx = pool.acquire().await?;
    //             let query = r#"Update jobs
    //                     INNER JOIN ( SELECT workers.id as worker_id, jobs.id as job_id from workers INNER JOIN jobs ON jobs.lock_by = workers.id WHERE jobs.status = "Running" AND workers.last_seen < ? AND workers.worker_type = ?
    //                         ORDER BY lock_at ASC LIMIT ?) as workers ON jobs.lock_by = workers.worker_id AND jobs.id = workers.job_id
    //                     SET status = "Pending", done_at = NULL, lock_by = NULL, lock_at = NULL, last_error ="Job was abandoned";"#;
    //             #[cfg(feature = "chrono")]
    //             let seconds_ago =
    //                 chrono::Utc::now() - chrono::Duration::seconds(timeout_worker.as_secs() as _);
    //             #[cfg(all(not(feature = "chrono"), feature = "time"))]
    //             let seconds_ago = time::OffsetDateTime::now_utc() - timeout_worker;
    //             sqlx::query(query)
    //                 .bind(seconds_ago)
    //                 .bind(job_type)
    //                 .bind(count)
    //                 .execute(&mut *tx)
    //                 .await?;
    //             Ok(true)
    //         }
    //         _ => todo!(),
    //     }
    // }

    // async fn kill(&mut self, worker_id: &WorkerId, job_id: &TaskId) -> Result<(), sqlx::Error> {
    //     let pool = self.pool.clone();

    //     let mut tx = pool.acquire().await?;
    //     let query =
    //         "UPDATE jobs SET status = 'Killed', done_at = NOW() WHERE id = ? AND lock_by = ?";
    //     sqlx::query(query)
    //         .bind(job_id.to_string())
    //         .bind(worker_id.to_string())
    //         .execute(&mut *tx)
    //         .await?;
    //     Ok(())
    // }

    /// Puts the job instantly back into the queue
    /// Another [Worker] may consume
    // async fn retry(&mut self, worker_id: &WorkerId, job_id: &TaskId) -> Result<(), sqlx::Error> {
    //     let pool = self.pool.clone();

    //     let mut tx = pool.acquire().await?;
    //     let query =
    //             "UPDATE jobs SET status = 'Pending', done_at = NULL, lock_by = NULL WHERE id = ? AND lock_by = ?";
    //     sqlx::query(query)
    //         .bind(job_id.to_string())
    //         .bind(worker_id.to_string())
    //         .execute(&mut *tx)
    //         .await?;
    //     Ok(())
    // }

    async fn len(&self) -> Result<i64, sqlx::Error> {
        let pool = self.pool.clone();

        let query = "Select Count(*) as count from jobs where status='Pending'";
        let record = sqlx::query(query).fetch_one(&pool).await?;
        Ok(record.try_get("count")?)
    }
    // async fn ack(&mut self, worker_id: &WorkerId, task_id: &TaskId) -> Result<(), sqlx::Error> {
    //     let pool = self.pool.clone();

    //     let query = "UPDATE jobs SET status = 'Done', done_at = now() WHERE id = ? AND lock_by = ?";
    //     sqlx::query(query)
    //         .bind(job_id.to_string())
    //         .bind(worker_id.to_string())
    //         .execute(&pool)
    //         .await?;
    //     Ok(())
    // }

    async fn reschedule(&mut self, job: Request<T>, wait: Duration) -> Result<(), sqlx::Error> {
        let pool = self.pool.clone();
        let job_id = job.id();

        let wait: i64 = wait.as_secs().try_into().unwrap();
        let mut tx = pool.acquire().await?;
        let query =
                "UPDATE jobs SET status = 'Pending', done_at = NULL, lock_by = NULL, lock_at = NULL, run_at = ? WHERE id = ?";

        sqlx::query(query)
            .bind(Utc::now().timestamp().add(wait))
            .bind(job_id.to_string())
            .execute(&mut *tx)
            .await?;
        Ok(())
    }

    async fn update(&self, job: Request<Self::Job>) -> Result<(), sqlx::Error> {
        let pool = self.pool.clone();
        let status = job.status().as_ref().to_string();
        let attempts = job.attempts();
        let done_at = *job.done_at();
        let lock_by = job.lock_by().clone();
        let lock_at = *job.lock_at();
        let last_error = job.last_error().clone();

        let mut tx = pool.acquire().await?;
        let query =
                "UPDATE jobs SET status = ?, attempts = ?, done_at = ?, lock_by = ?, lock_at = ?, last_error = ? WHERE id = ?";
        sqlx::query(query)
            .bind(status.to_owned())
            .bind(attempts)
            .bind(done_at)
            .bind(lock_by.map(|w| w.name().to_string()))
            .bind(lock_at)
            .bind(last_error)
            .bind(job_id.to_string())
            .execute(&mut *tx)
            .await?;
        Ok(())
    }

    fn is_empty(&self) -> impl futures::prelude::Future<Output = Result<bool, Self::Error>> + Send {
        todo!()
    }

    // async fn keep_alive<Service>(&mut self, worker_id: &WorkerId) -> Result<(), sqlx::Error> {
    //     self.keep_alive_at::<Service>(worker_id, Utc::now().timestamp())
    //         .await
    // }
}

#[cfg(test)]
mod tests {

    use super::*;
    use apalis_core::context::HasJobContext;
    use apalis_core::request::JobState;
    use email_service::Email;
    use futures::StreamExt;

    /// migrate DB and return a storage instance.
    async fn setup() -> MysqlStorage<Email> {
        let db_url = &std::env::var("DATABASE_URL").expect("No DATABASE_URL is specified");
        // Because connections cannot be shared across async runtime
        // (different runtimes are created for each test),
        // we don't share the storage and tests must be run sequentially.
        let storage = MysqlStorage::connect(db_url)
            .await
            .expect("DATABASE_URL is wrong");
        storage.setup().await.expect("failed to migrate DB");
        storage
    }

    /// rollback DB changes made by tests.
    /// Delete the following rows:
    ///  - jobs whose state is `Pending` or locked by `worker_id`
    ///  - worker identified by `worker_id`
    ///
    /// You should execute this function in the end of a test
    async fn cleanup(storage: MysqlStorage<Email>, worker_id: &WorkerId) {
        sqlx::query("DELETE FROM jobs WHERE lock_by = ? OR status = 'Pending'")
            .bind(worker_id.to_string())
            .execute(&storage.pool)
            .await
            .expect("failed to delete jobs");
        sqlx::query("DELETE FROM workers WHERE id = ?")
            .bind(worker_id.to_string())
            .execute(&storage.pool)
            .await
            .expect("failed to delete worker");
    }

    async fn consume_one<S, T>(storage: &mut S, worker_id: &WorkerId) -> JobRequest<T>
    where
        S: Storage<Output = T>,
    {
        let mut stream = storage.consume(worker_id, std::time::Duration::from_secs(10), 1);
        stream
            .next()
            .await
            .expect("stream is empty")
            .expect("failed to poll job")
            .expect("no job is pending")
    }

    fn example_email() -> Email {
        Email {
            subject: "Test Subject".to_string(),
            to: "example@mysql".to_string(),
            text: "Some Text".to_string(),
        }
    }

    struct DummyService {}

    async fn register_worker_at(
        storage: &mut MysqlStorage<Email>,
        last_seen: Timestamp,
    ) -> WorkerId {
        let worker_id = WorkerId::new("test-worker");

        storage
            .keep_alive_at::<DummyService>(&worker_id, last_seen)
            .await
            .expect("failed to register worker");
        worker_id
    }

    async fn register_worker(storage: &mut MysqlStorage<Email>) -> WorkerId {
        #[cfg(feature = "chrono")]
        let now = chrono::Utc::now();
        #[cfg(all(not(feature = "chrono"), feature = "time"))]
        let now = time::OffsetDateTime::now_utc();

        register_worker_at(storage, now).await
    }

    async fn push_email<S>(storage: &mut S, email: Email)
    where
        S: Storage<Output = Email>,
    {
        storage.push(email).await.expect("failed to push a job");
    }

    async fn get_job<S>(storage: &mut S, job_id: &TaskId) -> JobRequest<Email>
    where
        S: Storage<Output = Email>,
    {
        storage
            .fetch_by_id(job_id)
            .await
            .expect("failed to fetch job by id")
            .expect("no job found by id")
    }

    #[tokio::test]
    async fn test_consume_last_pushed_job() {
        let mut storage = setup().await;
        push_email(&mut storage, example_email()).await;

        let worker_id = register_worker(&mut storage).await;

        let job = consume_one(&mut storage, &worker_id).await;

        assert_eq!(*job.context().status(), JobState::Running);
        assert_eq!(*job.context().lock_by(), Some(worker_id.clone()));
        assert!(job.context().lock_at().is_some());

        cleanup(storage, &worker_id).await;
    }

    #[tokio::test]
    async fn test_acknowledge_job() {
        let mut storage = setup().await;
        push_email(&mut storage, example_email()).await;

        let worker_id = register_worker(&mut storage).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let job_id = job.context().id();

        storage
            .ack(&worker_id, job_id)
            .await
            .expect("failed to acknowledge the job");

        let job = get_job(&mut storage, job_id).await;
        assert_eq!(*job.context().status(), JobState::Done);
        assert!(job.context().done_at().is_some());

        cleanup(storage, &worker_id).await;
    }

    #[tokio::test]
    async fn test_kill_job() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let worker_id = register_worker(&mut storage).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let job_id = job.context().id();

        storage
            .kill(&worker_id, job_id)
            .await
            .expect("failed to kill job");

        let job = get_job(&mut storage, job_id).await;
        assert_eq!(*job.context().status(), JobState::Killed);
        assert!(job.context().done_at().is_some());

        cleanup(storage, &worker_id).await;
    }

    #[tokio::test]
    async fn test_storage_heartbeat_reenqueuorphaned_pulse_last_seen_6min() {
        let mut storage = setup().await;

        // push an Email job
        storage
            .push(example_email())
            .await
            .expect("failed to push job");

        // register a worker not responding since 6 minutes ago
        let worker_id = WorkerId::new("test_worker");
        #[cfg(feature = "chrono")]
        let six_minutes_ago = chrono::Utc::now() - chrono::Duration::minutes(6);
        #[cfg(all(not(feature = "chrono"), feature = "time"))]
        let six_minutes_ago = time::OffsetDateTime::now_utc() - time::Duration::minutes(6);

        storage
            .keep_alive_at::<Email>(&worker_id, six_minutes_ago)
            .await
            .unwrap();

        // fetch job
        let job = consume_one(&mut storage, &worker_id).await;
        assert_eq!(*job.context().status(), JobState::Running);

        // heartbeat with ReenqueueOrpharned pulse
        storage
            .heartbeat(StorageWorkerPulse::ReenqueueOrphaned {
                count: 5,
                timeout_worker: Duration::from_secs(300),
            })
            .await
            .unwrap();

        // then, the job status has changed to Pending
        let job = storage.fetch_by_id(job.id()).await.unwrap().unwrap();
        let context = job.context();
        assert_eq!(*context.status(), JobState::Pending);
        assert!(context.lock_by().is_none());
        assert!(context.lock_at().is_none());
        assert!(context.done_at().is_none());
        assert_eq!(*context.last_error(), Some("Job was abandoned".to_string()));

        cleanup(storage, &worker_id).await;
    }

    #[tokio::test]
    async fn test_storage_heartbeat_reenqueuorphaned_pulse_last_seen_4min() {
        let mut storage = setup().await;

        // push an Email job
        storage
            .push(example_email())
            .await
            .expect("failed to push job");

        // register a worker responding at 4 minutes ago
        #[cfg(feature = "chrono")]
        let four_minutes_ago = chrono::Utc::now() - chrono::Duration::minutes(4);
        #[cfg(all(not(feature = "chrono"), feature = "time"))]
        let four_minutes_ago = time::OffsetDateTime::now_utc() - time::Duration::minutes(4);

        let worker_id = WorkerId::new("test_worker");
        storage
            .keep_alive_at::<Email>(&worker_id, four_minutes_ago)
            .await
            .unwrap();

        // fetch job
        let job = consume_one(&mut storage, &worker_id).await;
        assert_eq!(*job.context().status(), JobState::Running);

        // heartbeat with ReenqueueOrpharned pulse
        storage
            .heartbeat(StorageWorkerPulse::ReenqueueOrphaned {
                count: 5,
                timeout_worker: Duration::from_secs(300),
            })
            .await
            .unwrap();

        // then, the job status is not changed
        let job = storage.fetch_by_id(job.id()).await.unwrap().unwrap();
        let context = job.context();
        assert_eq!(*context.status(), JobState::Running);
        assert_eq!(*context.lock_by(), Some(worker_id.clone()));

        cleanup(storage, &worker_id).await;
    }
}
