//! Allows persisting jobs on a postgres db.
//! Comes with helper functions eg:
//! ## Sql Example:
//!
//! ```sql
//! Select
//!     apalis.push_job(
//!             'apalis::Email',
//!              json_build_object('subject', 'Test Apalis', 'to', 'test1@example.com', 'text', 'Lorem Ipsum')
//!     );
//! ```

use apalis_core::error::{JobError, JobStreamError};
use apalis_core::job::{Counts, Job, JobStreamExt, JobStreamResult, JobStreamWorker};
use apalis_core::request::{JobRequest, JobState};
use apalis_core::storage::StorageError;
use apalis_core::storage::StorageWorkerPulse;
use apalis_core::storage::{Storage, StorageResult};
use async_stream::try_stream;
use chrono::{DateTime, Utc};
use futures::{FutureExt, Stream};
use futures_lite::future;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::postgres::PgListener;
use sqlx::types::Uuid;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::convert::TryInto;
use std::{marker::PhantomData, ops::Add, time::Duration};

use crate::from_row::{IntoJobRequest, SqlJobRequest};

/// Represents a [Storage] that persists to Postgres
#[derive(Debug)]
pub struct PostgresStorage<T> {
    pool: PgPool,
    job_type: PhantomData<T>,
}

impl<T> Clone for PostgresStorage<T> {
    fn clone(&self) -> Self {
        PostgresStorage {
            pool: self.pool.clone(),
            job_type: PhantomData,
        }
    }
}

impl<T> PostgresStorage<T> {
    /// New Storage from [PgPool]
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            job_type: PhantomData,
        }
    }

    /// Create a new storage instance
    pub async fn connect<S: Into<String>>(db: S) -> Result<Self, sqlx::Error> {
        let pool = PgPool::connect(&db.into()).await?;
        Ok(Self::new(pool))
    }

    /// Do migrations for Postgres
    #[cfg(feature = "migrate")]
    pub async fn setup(&self) -> Result<(), sqlx::Error> {
        let pool = self.pool.clone();
        sqlx::migrate!("migrations/postgres").run(&pool).await?;
        Ok(())
    }
}

impl<T: DeserializeOwned + Send + Unpin + Job> PostgresStorage<T> {
    fn stream_jobs(
        &self,
        worker_id: String,
        interval: Duration,
    ) -> impl Stream<Item = Result<Option<JobRequest<T>>, JobStreamError>> {
        let pool = self.pool.clone();
        let mut interval = tokio::time::interval(interval);

        try_stream! {
            let mut listener = PgListener::connect_with(&pool).await.map_err(|e| JobStreamError::BrokenPipe(Box::from(e)))?;
            let _notify = listener.listen("apalis::job").await.map_err(|e| JobStreamError::BrokenPipe(Box::from(e)))?;
            loop {
                //  Ideally wait for a job or a tick
                let interval = interval.tick().map(|_| ());
                let notification = listener.recv().map(|_| ()); // TODO: This silences errors from pubsub, needs improvement to trigger start queue
                future::race(interval, notification).await;
                let tx = pool.clone();
                let mut tx = tx.acquire().await.map_err(|e| JobStreamError::BrokenPipe(Box::from(e)))?;
                let job_type = T::NAME;
                let fetch_query = "Select * FROM apalis.get_job($1, $2) WHERE job IS NOT NULL;";
                let job: Option<SqlJobRequest<T>> = sqlx::query_as(fetch_query)
                    .bind(worker_id.clone())
                    .bind(job_type)
                    .fetch_optional(&mut tx)
                    .await.map_err(|e| JobStreamError::BrokenPipe(Box::from(e)))?;
                yield job.build_job_request()
            }
        }
    }
}

#[async_trait::async_trait]
impl<T> Storage for PostgresStorage<T>
where
    T: Job + Serialize + DeserializeOwned + Send + 'static + Unpin,
{
    type Output = T;

    /// Push a job to Postgres [Storage]
    ///
    /// # SQL Example
    ///
    /// ```sql
    /// Select apalis.push_job(job_type::text, job::json);
    /// ```
    async fn push(&mut self, job: Self::Output) -> StorageResult<()> {
        let id = Uuid::new_v4();
        let query = "INSERT INTO apalis.jobs VALUES ($1, $2, $3, 'Pending', 0, 25, NOW() , NULL, NULL, NULL, NULL)";
        let pool = self.pool.clone();
        let job = serde_json::to_value(&job)?;
        let mut pool = pool
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        let job_type = T::NAME;
        sqlx::query(query)
            .bind(job)
            .bind(id.to_string())
            .bind(job_type.to_string())
            .execute(&mut pool)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(())
    }

    async fn schedule(
        &mut self,
        job: Self::Output,
        on: chrono::DateTime<Utc>,
    ) -> StorageResult<()> {
        let query =
            "INSERT INTO apalis.jobs VALUES ($1, $2, $3, 'Pending', 0, 25, $4, NULL, NULL, NULL, NULL)";
        let pool = self.pool.clone();
        let id = Uuid::new_v4();
        let job = serde_json::to_value(&job)?;
        let mut pool = pool
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        let job_type = T::NAME;
        sqlx::query(query)
            .bind(job)
            .bind(id.to_string())
            .bind(job_type)
            .bind(on)
            .execute(&mut pool)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(())
    }

    async fn fetch_by_id(&self, job_id: String) -> StorageResult<Option<JobRequest<Self::Output>>> {
        let pool = self.pool.clone();

        let mut conn = pool
            .clone()
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        let fetch_query = "SELECT * FROM apalis.jobs WHERE id = $1";
        let res = sqlx::query_as(fetch_query)
            .bind(job_id)
            .fetch_optional(&mut conn)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(res.build_job_request())
    }

    async fn heartbeat(&mut self, pulse: StorageWorkerPulse) -> StorageResult<bool> {
        let pool = self.pool.clone();
        match pulse {
            StorageWorkerPulse::EnqueueScheduled { count: _ } => {
                // Idealy jobs are queue via run_at. So this is not necessary
                Ok(true)
            }
            // Worker not seen in 5 minutes yet has running jobs
            StorageWorkerPulse::RenqueueOrpharned { count } => {
                let job_type = T::NAME;
                let mut tx = pool
                    .acquire()
                    .await
                    .map_err(|e| StorageError::Database(Box::from(e)))?;
                let query = "Update apalis.jobs 
                            SET status = 'Pending', done_at = NULL, lock_by = NULL, lock_at = NULL, last_error ='Job was abandoned'
                            WHERE id in 
                                (SELECT jobs.id from apalis.jobs INNER join apalis.workers ON lock_by = workers.id 
                                    WHERE status= 'Running' AND workers.last_seen < NOW() - INTERVAL '5 minutes'
                                    AND workers.worker_type = $1 ORDER BY lock_at ASC LIMIT $2);";
                sqlx::query(query)
                    .bind(job_type)
                    .bind(count)
                    .execute(&mut tx)
                    .await
                    .map_err(|e| StorageError::Database(Box::from(e)))?;
                Ok(true)
            }
            _ => todo!(),
        }
    }

    async fn kill(&mut self, worker_id: String, job_id: String) -> StorageResult<()> {
        let pool = self.pool.clone();

        let mut tx = pool
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        let query =
                "UPDATE apalis.jobs SET status = 'Killed', done_at = now() WHERE id = $1 AND lock_by = $2";
        sqlx::query(query)
            .bind(job_id.to_owned())
            .bind(worker_id.to_owned())
            .execute(&mut tx)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(())
    }

    /// Puts the job instantly back into the queue
    /// Another [Worker] may consume
    async fn retry(&mut self, worker_id: String, job_id: String) -> StorageResult<()> {
        let pool = self.pool.clone();

        let mut tx = pool
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        let query =
                "UPDATE apalis.jobs SET status = 'Pending', done_at = NULL, lock_by = NULL WHERE id = $1 AND lock_by = $2";
        sqlx::query(query)
            .bind(job_id.to_owned())
            .bind(worker_id.to_owned())
            .execute(&mut tx)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(())
    }

    fn consume(&mut self, worker_id: String, interval: Duration) -> JobStreamResult<T> {
        Box::pin(self.stream_jobs(worker_id, interval))
    }
    async fn len(&self) -> StorageResult<i64> {
        let pool = self.pool.clone();

        let mut tx = pool
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        let query = "Select Count(*) as count from apalis.jobs where status='Pending'";
        let record = sqlx::query(query)
            .fetch_one(&mut tx)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(record
            .try_get("count")
            .map_err(|e| StorageError::Database(Box::from(e)))?)
    }
    async fn ack(&mut self, worker_id: String, job_id: String) -> StorageResult<()> {
        let pool = self.pool.clone();

        let mut tx = pool
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        let query =
                "UPDATE apalis.jobs SET status = 'Done', done_at = now() WHERE id = $1 AND lock_by = $2";
        sqlx::query(query)
            .bind(job_id.to_owned())
            .bind(worker_id.to_owned())
            .execute(&mut tx)
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
        let wait = chrono::Duration::seconds(wait);
        let mut tx = pool
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        let query =
                "UPDATE apalis.jobs SET status = 'Pending', done_at = NULL, lock_by = NULL, lock_at = NULL, run_at = $2 WHERE id = $1";
        sqlx::query(query)
            .bind(job_id.to_owned())
            .bind(Utc::now().add(wait))
            .execute(&mut tx)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(())
    }

    async fn update_by_id(
        &self,
        job_id: String,
        job: &JobRequest<Self::Output>,
    ) -> StorageResult<()> {
        let pool = self.pool.clone();
        let status = job.status().as_ref().to_string();
        let attempts = job.attempts();
        let done_at = *job.done_at();
        let lock_by = job.lock_by().clone();
        let lock_at = *job.lock_at();
        let last_error = job.last_error().clone();

        let mut tx = pool
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        let query =
                "UPDATE apalis.jobs SET status = $1, attempts = $2, done_at = $3, lock_by = $4, lock_at = $5, last_error = $6 WHERE id = $7";
        sqlx::query(query)
            .bind(status.to_owned())
            .bind(attempts)
            .bind(done_at)
            .bind(lock_by)
            .bind(lock_at)
            .bind(last_error)
            .bind(job_id.to_owned())
            .execute(&mut tx)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(())
    }

    async fn keep_alive<Service>(&mut self, worker_id: String) -> StorageResult<()> {
        let pool = self.pool.clone();

        let mut tx = pool
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        let worker_type = T::NAME;
        let storage_name = std::any::type_name::<Self>();
        let query =
                "INSERT INTO apalis.workers (id, worker_type, storage_name, layers, last_seen) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) 
                DO 
                   UPDATE SET last_seen = NOW()";
        sqlx::query(query)
            .bind(worker_id.to_owned())
            .bind(worker_type)
            .bind(storage_name)
            .bind(std::any::type_name::<Service>())
            .bind(Utc::now())
            .execute(&mut tx)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(())
    }
}

#[async_trait::async_trait]

impl<J: 'static + Job + Serialize + DeserializeOwned> JobStreamExt<J> for PostgresStorage<J> {
    async fn counts(&mut self) -> Result<Counts, JobError> {
        let mut conn = self
            .pool
            .clone()
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;

        let fetch_query = "SELECT
                            COUNT(1) FILTER (WHERE status = 'Pending') AS pending, 
                            COUNT(1) FILTER (WHERE status = 'Running') AS running,
                            COUNT(1) FILTER (WHERE status = 'Done') AS done,
                            COUNT(1) FILTER (WHERE status = 'Retry') AS retry, 
                            COUNT(1) FILTER (WHERE status = 'Failed') AS failed, 
                            COUNT(1) FILTER (WHERE status = 'Killed') AS killed
                        FROM apalis.jobs WHERE job_type = $1";
        let res: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(fetch_query)
            .bind(J::NAME)
            .fetch_one(&mut conn)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        let mut inner = HashMap::new();
        inner.insert(JobState::Pending, res.0);
        inner.insert(JobState::Running, res.1);
        inner.insert(JobState::Done, res.2);
        inner.insert(JobState::Retry, res.3);
        inner.insert(JobState::Failed, res.4);
        inner.insert(JobState::Killed, res.5);
        Ok(Counts { inner })
    }

    async fn list_jobs(
        &mut self,
        status: &JobState,
        page: i32,
    ) -> Result<Vec<JobRequest<J>>, JobError> {
        let status = status.as_ref().to_string();

        let mut conn = self
            .pool
            .clone()
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        let fetch_query = "SELECT * FROM apalis.jobs WHERE status = $1 AND job_type = $2 ORDER BY done_at DESC, run_at DESC LIMIT 10 OFFSET $3";
        let res: Vec<SqlJobRequest<J>> = sqlx::query_as(fetch_query)
            .bind(status)
            .bind(J::NAME)
            .bind((page - 1) * 10)
            .fetch_all(&mut conn)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(res.into_iter().map(|j| j.into()).collect())
    }

    async fn list_workers(&mut self) -> Result<Vec<JobStreamWorker>, JobError> {
        let mut conn = self
            .pool
            .clone()
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        let fetch_query =
            "SELECT id, layers, last_seen FROM apalis.workers WHERE worker_type = $1 ORDER BY last_seen DESC LIMIT 20 OFFSET $2";
        let res: Vec<(String, String, DateTime<Utc>)> = sqlx::query_as(fetch_query)
            .bind(J::NAME)
            .bind(0_i64)
            .fetch_all(&mut conn)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(res
            .into_iter()
            .map(|(worker_id, layers, last_seen)| {
                let mut worker = JobStreamWorker::new::<Self, J>(worker_id, last_seen);
                worker.set_layers(layers);
                worker
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::DerefMut;

    use once_cell::sync::OnceCell;
    use tokio::sync::Mutex;
    use tokio::sync::MutexGuard;

    use super::*;
    use email_service::Email;
    use futures::StreamExt;

    async fn setup<'a>() -> MutexGuard<'a, PostgresStorage<Email>> {
        static INSTANCE: OnceCell<Mutex<PostgresStorage<Email>>> = OnceCell::new();
        let mutex = INSTANCE.get_or_init(|| {
            let db_url = &std::env::var("DATABASE_URL").expect("No DATABASE_URL is specified");
            let pool = PgPool::connect_lazy(db_url).expect("DATABASE_URL is wrong");
            Mutex::new(PostgresStorage::new(pool))
        });
        let storage = mutex.lock().await;
        storage.setup().await.expect("failed to run migrations");
        storage
    }

    fn example_email() -> Email {
        Email {
            subject: "Test Subject".to_string(),
            to: "example@postgres".to_string(),
            text: "Some Text".to_string(),
        }
    }

    struct DummyService {}

    async fn consume_one<S, T>(storage: &mut S, worker_id: String) -> JobRequest<T>
    where
        S: Storage<Output = T>,
    {
        let mut stream = storage.consume(worker_id, std::time::Duration::from_secs(10));
        stream
            .next()
            .await
            .expect("stream is empty")
            .expect("failed to poll job")
            .expect("no job is pending")
    }

    async fn register_worker<S, T>(storage: &mut S) -> String
    where
        S: Storage<Output = T>,
    {
        let worker_id = Uuid::new_v4().to_string();

        storage
            .keep_alive::<DummyService>(worker_id.clone())
            .await
            .expect("failed to register worker");
        worker_id
    }

    async fn push_email<S>(storage: &mut S, email: Email)
    where
        S: Storage<Output = Email>,
    {
        storage.push(email).await.expect("failed to push a job");
    }

    async fn get_job<S>(storage: &mut S, job_id: String) -> JobRequest<Email>
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
        let storage = storage.deref_mut();
        push_email(storage, example_email()).await;

        let worker_id = register_worker(storage).await;

        let job = consume_one(storage, worker_id.clone()).await;

        assert_eq!(*job.context().status(), JobState::Running);
        assert_eq!(*job.context().lock_by(), Some(worker_id.clone()));
        assert!(job.context().lock_at().is_some());
    }

    #[tokio::test]
    async fn test_acknowledge_job() {
        let mut storage = setup().await;
        let storage = storage.deref_mut();
        push_email(storage, example_email()).await;

        let worker_id = register_worker(storage).await;

        let job = consume_one(storage, worker_id.clone()).await;
        let job_id = job.context().id();

        storage
            .ack(worker_id.clone(), job_id.clone())
            .await
            .expect("failed to acknowledge the job");

        let job = get_job(storage, job_id.clone()).await;
        assert_eq!(*job.context().status(), JobState::Done);
        assert!(job.context().done_at().is_some());
    }

    #[tokio::test]
    async fn test_kill_job() {
        let mut storage = setup().await;
        let storage = storage.deref_mut();

        push_email(storage, example_email()).await;

        let worker_id = register_worker(storage).await;

        let job = consume_one(storage, worker_id.clone()).await;
        let job_id = job.context().id();

        storage
            .kill(worker_id.clone(), job_id.clone())
            .await
            .expect("failed to kill job");

        let job = get_job(storage, job_id.clone()).await;
        assert_eq!(*job.context().status(), JobState::Killed);
        assert!(job.context().done_at().is_some());
    }
}
