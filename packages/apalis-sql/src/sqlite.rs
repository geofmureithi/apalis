use crate::from_row::IntoJobRequest;
use apalis_core::error::JobStreamError;
use apalis_core::job::{Job, JobId, JobStreamResult};
use apalis_core::request::JobRequest;
use apalis_core::storage::StorageError;
use apalis_core::storage::StorageWorkerPulse;
use apalis_core::storage::{Storage, StorageResult};
use apalis_core::utils::Timer;
use apalis_core::worker::WorkerId;
use async_stream::try_stream;
use futures::Stream;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::{Pool, Row, Sqlite, SqlitePool};
use std::convert::TryInto;
use std::{marker::PhantomData, ops::Add, time::Duration};

use crate::{from_row::SqlJobRequest, Timestamp};

/// Represents a [Storage] that persists to Sqlite
#[derive(Debug)]
pub struct SqliteStorage<T> {
    pool: Pool<Sqlite>,
    job_type: PhantomData<T>,
}

impl<T> Clone for SqliteStorage<T> {
    fn clone(&self) -> Self {
        let pool = self.pool.clone();
        SqliteStorage {
            pool,
            job_type: PhantomData,
        }
    }
}

impl<T: Job> SqliteStorage<T> {
    /// Construct a new Storage from a pool
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            job_type: PhantomData,
        }
    }
    /// Connect to a database given a url
    pub async fn connect<S: Into<String>>(db: S) -> Result<Self, sqlx::Error> {
        let pool = SqlitePool::connect(&db.into()).await?;
        Ok(Self::new(pool))
    }

    /// Perform migrations for storage
    #[cfg(feature = "migrate")]
    pub async fn setup(&self) -> Result<(), sqlx::Error> {
        let pool = self.pool.clone();
        sqlx::query("PRAGMA journal_mode = 'WAL';")
            .execute(&pool)
            .await?;
        sqlx::query("PRAGMA temp_store = 2;").execute(&pool).await?;
        sqlx::query("PRAGMA synchronous = NORMAL;")
            .execute(&pool)
            .await?;
        sqlx::query("PRAGMA cache_size = 64000;")
            .execute(&pool)
            .await?;
        Self::migrations().run(&pool).await?;
        Ok(())
    }

    /// Get sqlite migrations without running them
    #[cfg(feature = "migrate")]
    pub fn migrations() -> sqlx::migrate::Migrator {
        sqlx::migrate!("migrations/sqlite")
    }

    /// Keeps a storage notified that the worker is still alive manually
    pub async fn keep_alive_at<Service>(
        &mut self,
        worker_id: &WorkerId,
        last_seen: Timestamp,
    ) -> StorageResult<()> {
        let pool = self.pool.clone();
        let worker_type = T::NAME;
        let storage_name = std::any::type_name::<Self>();
        let query = "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (id) DO
                   UPDATE SET last_seen = EXCLUDED.last_seen";
        #[cfg(feature = "chrono")]
        let last_seen = last_seen.timestamp();
        #[cfg(all(not(feature = "chrono"), feature = "time"))]
        let last_seen = last_seen.unix_timestamp();
        sqlx::query(query)
            .bind(worker_id.to_string())
            .bind(worker_type)
            .bind(storage_name)
            .bind(std::any::type_name::<Service>())
            .bind(last_seen)
            .execute(&pool)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(())
    }

    /// Expose the pool for other functionality, eg custom migrations
    pub fn pool(&self) -> &Pool<Sqlite> {
        &self.pool
    }
}

async fn fetch_next<T>(
    pool: Pool<Sqlite>,
    worker_id: &WorkerId,
    job: Option<JobRequest<T>>,
) -> Result<Option<JobRequest<T>>, JobStreamError>
where
    T: Job + Send + Unpin + DeserializeOwned,
{
    match job {
        None => Ok(None),
        Some(job) => {
            #[cfg(feature = "chrono")]
            let now = chrono::Utc::now().timestamp();
            #[cfg(all(not(feature = "chrono"), feature = "time"))]
            let now = time::OffsetDateTime::now_utc().unix_timestamp();

            let job_id = job.id();
            let update_query = "UPDATE Jobs SET status = 'Running', lock_by = ?2, lock_at = ?3 WHERE id = ?1 AND job_type = ?4 AND status = 'Pending' AND lock_by IS NULL; Select * from Jobs where id = ?1 AND lock_by = ?2 AND job_type = ?4";
            let job: Option<SqlJobRequest<T>> = sqlx::query_as(update_query)
                .bind(job_id.to_string())
                .bind(worker_id.to_string())
                .bind(now)
                .bind(T::NAME)
                .fetch_optional(&pool)
                .await
                .map_err(|e| JobStreamError::BrokenPipe(Box::from(e)))?;

            Ok(job.build_job_request())
        }
    }
}

impl<T: DeserializeOwned + Send + Unpin + Job> SqliteStorage<T> {
    fn stream_jobs(
        &self,
        worker_id: &WorkerId,
        interval: Duration,
        buffer_size: usize,
    ) -> impl Stream<Item = Result<Option<JobRequest<T>>, JobStreamError>> {
        let pool = self.pool.clone();
        #[cfg(feature = "async-std-comp")]
        #[allow(unused_variables)]
        let sleeper = apalis_core::utils::timer::AsyncStdTimer;
        #[cfg(feature = "tokio-comp")]
        let sleeper = apalis_core::utils::timer::TokioTimer;
        let worker_id = worker_id.clone();
        try_stream! {
            loop {
                sleeper.sleep(interval).await;
                let tx = pool.clone();
                let mut tx = tx.acquire().await.map_err(|e| JobStreamError::BrokenPipe(Box::from(e)))?;
                #[cfg(feature = "chrono")]
                let now = chrono::Utc::now().timestamp();
                #[cfg(all(not(feature = "chrono"), feature = "time"))]
                let now = time::OffsetDateTime::now_utc().unix_timestamp();
                let job_type = T::NAME;
                let fetch_query = "SELECT * FROM Jobs
                    WHERE (status = 'Pending' OR (status = 'Failed' AND attempts < max_attempts)) AND run_at < ?1 AND job_type = ?2 LIMIT ?3";
                let jobs: Vec<SqlJobRequest<T>> = sqlx::query_as(fetch_query)
                    .bind(now)
                    .bind(job_type)
                    .bind(i64::try_from(buffer_size).map_err(|e| JobStreamError::BrokenPipe(Box::from(e)))?)
                    .fetch_all(&mut *tx)
                    .await.map_err(|e| JobStreamError::BrokenPipe(Box::from(e)))?;
                for job in jobs {
                    yield fetch_next(pool.clone(), &worker_id, job.build_job_request()).await?;
                }
            }
        }
    }
}
#[async_trait::async_trait]
impl<T> Storage for SqliteStorage<T>
where
    T: Job + Serialize + DeserializeOwned + Send + 'static + Unpin + Sync,
{
    type Output = T;

    async fn push(&mut self, job: Self::Output) -> StorageResult<JobId> {
        let id = JobId::new();
        let query = "INSERT INTO Jobs VALUES (?1, ?2, ?3, 'Pending', 0, 25, strftime('%s','now'), NULL, NULL, NULL, NULL)";
        let pool = self.pool.clone();

        let job = serde_json::to_string(&job).map_err(|e| StorageError::Parse(e.into()))?;
        let job_type = T::NAME;
        sqlx::query(query)
            .bind(job)
            .bind(id.to_string())
            .bind(job_type.to_string())
            .execute(&pool)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(id)
    }

    async fn schedule(&mut self, job: Self::Output, on: Timestamp) -> StorageResult<JobId> {
        let query =
            "INSERT INTO Jobs VALUES (?1, ?2, ?3, 'Pending', 0, 25, ?4, NULL, NULL, NULL, NULL)";
        let pool = self.pool.clone();
        let id = JobId::new();

        #[cfg(feature = "chrono")]
        let on = on.timestamp();
        #[cfg(all(not(feature = "chrono"), feature = "time"))]
        let on = on.unix_timestamp();

        let job = serde_json::to_string(&job).map_err(|e| StorageError::Parse(e.into()))?;
        let job_type = T::NAME;
        sqlx::query(query)
            .bind(job)
            .bind(id.to_string())
            .bind(job_type)
            .bind(on)
            .execute(&pool)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(id)
    }

    async fn fetch_by_id(&self, job_id: &JobId) -> StorageResult<Option<JobRequest<Self::Output>>> {
        let pool = self.pool.clone();
        let fetch_query = "SELECT * FROM Jobs WHERE id = ?1";
        let res: Option<SqlJobRequest<T>> = sqlx::query_as(fetch_query)
            .bind(job_id.to_string())
            .fetch_optional(&pool)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(res.build_job_request())
    }

    /// Used for scheduling jobs via [StorageWorkerPulse] signals
    async fn heartbeat(&mut self, pulse: StorageWorkerPulse) -> StorageResult<bool> {
        let pool = self.pool.clone();

        match pulse {
            StorageWorkerPulse::EnqueueScheduled { count } => {
                let job_type = T::NAME;
                let mut tx = pool
                    .acquire()
                    .await
                    .map_err(|e| StorageError::Database(Box::from(e)))?;
                let query = r#"Update Jobs
                            SET status = "Pending", done_at = NULL, lock_by = NULL, lock_at = NULL
                            WHERE id in
                                (SELECT Jobs.id from Jobs
                                    WHERE status= "Failed" AND Jobs.attempts < Jobs.max_attempts
                                     ORDER BY lock_at ASC LIMIT ?2);"#;
                sqlx::query(query)
                    .bind(job_type)
                    .bind(count)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Database(Box::from(e)))?;
                Ok(true)
            }
            // Worker not seen in 5 minutes yet has running jobs
            StorageWorkerPulse::ReenqueueOrphaned {
                count,
                timeout_worker,
            } => {
                let job_type = T::NAME;
                let mut tx = pool
                    .acquire()
                    .await
                    .map_err(|e| StorageError::Database(Box::from(e)))?;
                let query = r#"Update Jobs
                            SET status = "Pending", done_at = NULL, lock_by = NULL, lock_at = NULL, last_error ="Job was abandoned"
                            WHERE id in
                                (SELECT Jobs.id from Jobs INNER join Workers ON lock_by = Workers.id
                                    WHERE status= "Running" AND workers.last_seen < ?1
                                    AND Workers.worker_type = ?2 ORDER BY lock_at ASC LIMIT ?3);"#;
                #[cfg(feature = "chrono")]
                let seconds_ago = (chrono::Utc::now()
                    - chrono::Duration::seconds(timeout_worker.as_secs() as _))
                .timestamp();
                #[cfg(all(not(feature = "chrono"), feature = "time"))]
                let seconds_ago =
                    (time::OffsetDateTime::now_utc() - timeout_worker).unix_timestamp();
                sqlx::query(query)
                    .bind(seconds_ago)
                    .bind(job_type)
                    .bind(count)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Database(Box::from(e)))?;
                Ok(true)
            }
            _ => todo!(),
        }
    }

    async fn kill(&mut self, worker_id: &WorkerId, job_id: &JobId) -> StorageResult<()> {
        let pool = self.pool.clone();

        let mut tx = pool
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        let query =
                "UPDATE Jobs SET status = 'Killed', done_at = strftime('%s','now') WHERE id = ?1 AND lock_by = ?2";
        sqlx::query(query)
            .bind(job_id.to_string())
            .bind(worker_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(())
    }

    /// Puts the job instantly back into the queue
    /// Another [Worker] may consume
    async fn retry(&mut self, worker_id: &WorkerId, job_id: &JobId) -> StorageResult<()> {
        let pool = self.pool.clone();

        let mut tx = pool
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        let query =
                "UPDATE Jobs SET status = 'Pending', done_at = NULL, lock_by = NULL WHERE id = ?1 AND lock_by = ?2";
        sqlx::query(query)
            .bind(job_id.to_string())
            .bind(worker_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(())
    }

    fn consume(
        &mut self,
        worker_id: &WorkerId,
        interval: Duration,
        buffer_size: usize,
    ) -> JobStreamResult<T> {
        Box::pin(self.stream_jobs(worker_id, interval, buffer_size))
    }
    async fn len(&self) -> StorageResult<i64> {
        let pool = self.pool.clone();

        let query = "Select Count(*) as count from Jobs where status='Pending'";
        let record = sqlx::query(query)
            .fetch_one(&pool)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(record
            .try_get("count")
            .map_err(|e| StorageError::Database(Box::from(e)))?)
    }
    async fn ack(&mut self, worker_id: &WorkerId, job_id: &JobId) -> StorageResult<()> {
        let pool = self.pool.clone();
        let query =
                "UPDATE Jobs SET status = 'Done', done_at = strftime('%s','now') WHERE id = ?1 AND lock_by = ?2";
        sqlx::query(query)
            .bind(job_id.to_string())
            .bind(worker_id.to_string())
            .execute(&pool)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(())
    }

    async fn reschedule(&mut self, job: &JobRequest<T>, wait: Duration) -> StorageResult<()> {
        let pool = self.pool.clone();
        let job_id = job.id();

        let wait: i64 = wait
            .as_secs()
            .try_into()
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        #[cfg(feature = "chrono")]
        let wait = chrono::Duration::seconds(wait);
        #[cfg(all(not(feature = "chrono"), feature = "time"))]
        let wait = time::Duration::seconds(wait);
        let mut tx = pool
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        let query =
                "UPDATE Jobs SET status = 'Failed', done_at = NULL, lock_by = NULL, lock_at = NULL, run_at = ?2 WHERE id = ?1";
        #[cfg(feature = "chrono")]
        let wait_until = chrono::Utc::now().add(wait).timestamp();
        #[cfg(all(not(feature = "chrono"), feature = "time"))]
        let wait_until = time::OffsetDateTime::now_utc().add(wait).unix_timestamp();
        sqlx::query(query)
            .bind(job_id.to_string())
            .bind(wait_until)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(())
    }

    async fn update_by_id(
        &self,
        job_id: &JobId,
        job: &JobRequest<Self::Output>,
    ) -> StorageResult<()> {
        let pool = self.pool.clone();
        let status = job.status().as_ref().to_string();
        let attempts = job.attempts();
        #[cfg(feature = "chrono")]
        let done_at = (*job.done_at()).map(|v| v.timestamp());
        #[cfg(all(not(feature = "chrono"), feature = "time"))]
        let done_at = (*job.done_at()).map(|v| v.unix_timestamp());
        let lock_by = job.lock_by().clone();
        #[cfg(feature = "chrono")]
        let lock_at = (*job.lock_at()).map(|v| v.timestamp());
        #[cfg(all(not(feature = "chrono"), feature = "time"))]
        let lock_at = (*job.lock_at()).map(|v| v.unix_timestamp());
        let last_error = job.last_error().clone();

        let mut tx = pool
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        let query =
                "UPDATE Jobs SET status = ?1, attempts = ?2, done_at = ?3, lock_by = ?4, lock_at = ?5, last_error = ?6 WHERE id = ?7";
        sqlx::query(query)
            .bind(status.to_owned())
            .bind(attempts)
            .bind(done_at)
            .bind(lock_by.map(|w| w.name().to_string()))
            .bind(lock_at)
            .bind(last_error)
            .bind(job_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(())
    }

    async fn keep_alive<Service>(&mut self, worker_id: &WorkerId) -> StorageResult<()> {
        #[cfg(feature = "chrono")]
        let now = chrono::Utc::now();
        #[cfg(all(not(feature = "chrono"), feature = "time"))]
        let now = time::OffsetDateTime::now_utc();

        self.keep_alive_at::<Service>(worker_id, now).await
    }
}

#[cfg(feature = "expose")]
#[cfg_attr(docsrs, doc(cfg(feature = "expose")))]
/// Expose an [`SqliteStorage`] for web and cli management tools
pub mod expose {
    use super::*;
    use apalis_core::error::JobError;
    use apalis_core::expose::{ExposedWorker, JobStateCount, JobStreamExt};
    use apalis_core::request::JobState;
    use apalis_core::storage::StorageError;
    use std::collections::HashMap;

    #[async_trait::async_trait]
    impl<J: 'static + Job + Serialize + DeserializeOwned + Unpin + Send + Sync> JobStreamExt<J>
        for SqliteStorage<J>
    {
        async fn counts(&mut self) -> Result<JobStateCount, JobError> {
            let fetch_query = "SELECT
                            COUNT(1) FILTER (WHERE status = 'Pending') AS pending,
                            COUNT(1) FILTER (WHERE status = 'Running') AS running,
                            COUNT(1) FILTER (WHERE status = 'Done') AS done,
                            COUNT(1) FILTER (WHERE status = 'Retry') AS retry,
                            COUNT(1) FILTER (WHERE status = 'Failed') AS failed,
                            COUNT(1) FILTER (WHERE status = 'Killed') AS killed
                        FROM Jobs WHERE job_type = ?";
            let res: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(fetch_query)
                .bind(J::NAME)
                .fetch_one(self.pool())
                .await
                .map_err(|e| StorageError::Database(Box::from(e)))?;
            let mut inner = HashMap::new();
            inner.insert(JobState::Pending, res.0.try_into()?);
            inner.insert(JobState::Running, res.1.try_into()?);
            inner.insert(JobState::Done, res.2.try_into()?);
            inner.insert(JobState::Retry, res.3.try_into()?);
            inner.insert(JobState::Failed, res.4.try_into()?);
            inner.insert(JobState::Killed, res.5.try_into()?);
            Ok(JobStateCount::new(inner))
        }

        async fn list_jobs(
            &mut self,
            status: &JobState,
            page: i32,
        ) -> Result<Vec<JobRequest<J>>, JobError> {
            let status = status.as_ref().to_string();
            let fetch_query = "SELECT * FROM Jobs WHERE status = ? AND job_type = ? ORDER BY done_at DESC, run_at DESC LIMIT 10 OFFSET ?";
            let res: Vec<SqlJobRequest<J>> = sqlx::query_as(fetch_query)
                .bind(status)
                .bind(J::NAME)
                .bind(((page - 1) * 10).to_string())
                .fetch_all(self.pool())
                .await
                .map_err(|e| StorageError::Database(Box::from(e)))?;
            Ok(res.into_iter().map(|j| j.into()).collect())
        }

        async fn list_workers(&mut self) -> Result<Vec<ExposedWorker>, JobError> {
            let fetch_query =
            "SELECT id, layers, last_seen FROM Workers WHERE worker_type = ? ORDER BY last_seen DESC LIMIT 20 OFFSET ?";
            let res: Vec<(String, String, Timestamp)> = sqlx::query_as(fetch_query)
                .bind(J::NAME)
                .bind("0")
                .fetch_all(self.pool())
                .await
                .map_err(|e| StorageError::Database(Box::from(e)))?;
            Ok(res
                .into_iter()
                .map(|(worker_id, layers, last_seen)| {
                    ExposedWorker::new::<Self, J>(WorkerId::new(worker_id), layers, last_seen)
                })
                .collect())
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use apalis_core::context::HasJobContext;
    use apalis_core::request::JobState;
    use email_service::Email;
    use futures::StreamExt;

    /// migrate DB and return a storage instance.
    async fn setup() -> SqliteStorage<Email> {
        // Because connections cannot be shared across async runtime
        // (different runtimes are created for each test),
        // we don't share the storage and tests must be run sequentially.
        let storage = SqliteStorage::<Email>::connect("sqlite::memory:")
            .await
            .expect("failed to connect DB server");
        storage.setup().await.expect("failed to migrate DB");
        storage
    }

    #[tokio::test]
    async fn test_inmemory_sqlite_worker() {
        let mut sqlite = SqliteStorage::<Email>::connect("sqlite::memory:")
            .await
            .expect("Could not start inmemory storage");
        sqlite.setup().await.expect("Could not run migrations");
        sqlite
            .push(Email {
                subject: "Test Subject".to_string(),
                to: "example@sqlite".to_string(),
                text: "Some Text".to_string(),
            })
            .await
            .expect("Unable to push job");
        let len = sqlite.len().await.expect("Could not fetch the jobs count");
        assert_eq!(len, 1);
        // assert!(sqlite.is_empty().await.is_err())
    }

    struct DummyService {}

    fn example_email() -> Email {
        Email {
            subject: "Test Subject".to_string(),
            to: "example@postgres".to_string(),
            text: "Some Text".to_string(),
        }
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

    async fn register_worker_at(
        storage: &mut SqliteStorage<Email>,
        last_seen: Timestamp,
    ) -> WorkerId {
        let worker_id = WorkerId::new("test-worker");

        storage
            .keep_alive_at::<DummyService>(&worker_id, last_seen)
            .await
            .expect("failed to register worker");
        worker_id
    }

    async fn register_worker(storage: &mut SqliteStorage<Email>) -> WorkerId {
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

    async fn get_job<S>(storage: &mut S, job_id: &JobId) -> JobRequest<Email>
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
    }

    #[tokio::test]
    async fn test_heartbeat_renqueueorphaned_pulse_last_seen_6min() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        #[cfg(feature = "chrono")]
        let six_minutes_ago = chrono::Utc::now() - chrono::Duration::minutes(6);
        #[cfg(all(not(feature = "chrono"), feature = "time"))]
        let six_minutes_ago = time::OffsetDateTime::now_utc() - time::Duration::minutes(6);

        let worker_id = register_worker_at(&mut storage, six_minutes_ago).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let result = storage
            .heartbeat(StorageWorkerPulse::ReenqueueOrphaned {
                count: 5,
                timeout_worker: Duration::from_secs(300),
            })
            .await
            .expect("failed to heartbeat");
        assert!(result);

        let job_id = job.context().id();
        let job = get_job(&mut storage, job_id).await;

        assert_eq!(*job.context().status(), JobState::Pending);
        assert!(job.context().done_at().is_none());
        assert!(job.context().lock_by().is_none());
        assert!(job.context().lock_at().is_none());
        assert_eq!(
            *job.context().last_error(),
            Some("Job was abandoned".to_string())
        );
    }

    #[tokio::test]
    async fn test_heartbeat_renqueueorphaned_pulse_last_seen_4min() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        #[cfg(feature = "chrono")]
        let four_minutes_ago = chrono::Utc::now() - chrono::Duration::minutes(4);
        #[cfg(all(not(feature = "chrono"), feature = "time"))]
        let four_minutes_ago = time::OffsetDateTime::now_utc() - time::Duration::minutes(4);

        let worker_id = register_worker_at(&mut storage, four_minutes_ago).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let result = storage
            .heartbeat(StorageWorkerPulse::ReenqueueOrphaned {
                count: 5,
                timeout_worker: Duration::from_secs(300),
            })
            .await
            .expect("failed to heartbeat");
        assert!(result);

        let job_id = job.context().id();
        let job = get_job(&mut storage, job_id).await;

        assert_eq!(*job.context().status(), JobState::Running);
        assert_eq!(*job.context().lock_by(), Some(worker_id));
    }
}
