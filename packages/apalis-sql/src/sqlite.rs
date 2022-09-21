use crate::from_row::IntoJobRequest;
use apalis_core::error::{JobError, JobStreamError};
use apalis_core::job::{Counts, Job, JobStreamExt, JobStreamResult, JobStreamWorker};
use apalis_core::request::{JobRequest, JobState};
use apalis_core::storage::StorageError;
use apalis_core::storage::StorageWorkerPulse;
use apalis_core::storage::{Storage, StorageResult};
use async_stream::try_stream;
use chrono::{DateTime, Utc};
use futures::Stream;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::types::Uuid;
use sqlx::{Pool, Row, Sqlite, SqlitePool};
use std::collections::HashMap;
use std::convert::TryInto;
use std::ops::Sub;
use std::{marker::PhantomData, ops::Add, time::Duration};

use crate::from_row::SqlJobRequest;

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

impl<T> SqliteStorage<T> {
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
        sqlx::migrate!("migrations/sqlite").run(&pool).await?;
        Ok(())
    }
}

async fn fetch_next<T>(
    pool: Pool<Sqlite>,
    worker_id: String,
    job: Option<JobRequest<T>>,
) -> Result<Option<JobRequest<T>>, JobStreamError>
where
    T: Send + Unpin + DeserializeOwned,
{
    match job {
        None => Ok(None),
        Some(job) => {
            let mut tx = pool
                .begin()
                .await
                .map_err(|e| JobStreamError::BrokenPipe(Box::from(e)))?;
            let job_id = job.id();
            let update_query = "UPDATE Jobs SET status = 'Running', lock_by = ?2 WHERE id = ?1 AND status = 'Pending' AND lock_by IS NULL; Select * from Jobs where id = ?1 AND lock_by = ?2";
            let job: Option<SqlJobRequest<T>> = sqlx::query_as(update_query)
                .bind(job_id.clone())
                .bind(worker_id)
                .fetch_optional(&mut tx)
                .await
                .map_err(|e| JobStreamError::BrokenPipe(Box::from(e)))?;
            tx.commit()
                .await
                .map_err(|e| JobStreamError::BrokenPipe(Box::from(e)))?;
            Ok(job.build_job_request())
        }
    }
}

impl<T: DeserializeOwned + Send + Unpin + Job> SqliteStorage<T> {
    fn stream_jobs(
        &self,
        worker_id: String,
        interval: Duration,
    ) -> impl Stream<Item = Result<Option<JobRequest<T>>, JobStreamError>> {
        let pool = self.pool.clone();
        let mut interval = tokio::time::interval(interval);
        try_stream! {
            loop {
                interval.tick().await;
                let tx = pool.clone();
                let mut tx = tx.acquire().await.map_err(|e| JobStreamError::BrokenPipe(Box::from(e)))?;
                let job_type = T::NAME;
                let fetch_query = "SELECT * FROM Jobs
                    WHERE rowid = (SELECT min(rowid) FROM Jobs
                    WHERE status = 'Pending' OR (status = 'Failed' AND attempts < max_attempts) AND run_at < ?1 AND job_type = ?2)";
                let job: Option<SqlJobRequest<T>> = sqlx::query_as(fetch_query)
                    .bind(Utc::now().timestamp())
                    .bind(job_type)
                    .fetch_optional(&mut tx)
                    .await.map_err(|e| JobStreamError::BrokenPipe(Box::from(e)))?;
                yield fetch_next(pool.clone(), worker_id.clone(), job.build_job_request()).await?
            }
        }
    }
}
#[async_trait::async_trait]
impl<T> Storage for SqliteStorage<T>
where
    T: Job + Serialize + DeserializeOwned + Send + 'static + Unpin,
{
    type Output = T;

    async fn push(&mut self, job: Self::Output) -> StorageResult<()> {
        let id = Uuid::new_v4();
        let query = "INSERT INTO Jobs VALUES (?1, ?2, ?3, 'Pending', 0, 25, strftime('%s','now'), NULL, NULL, NULL, NULL)";
        let pool = self.pool.clone();

        let job = serde_json::to_string(&job)?;
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
            "INSERT INTO Jobs VALUES (?1, ?2, ?3, 'Pending', 0, 25, ?4, NULL, NULL, NULL, NULL)";
        let pool = self.pool.clone();
        let id = Uuid::new_v4();

        let job = serde_json::to_string(&job)?;
        let mut pool = pool
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        let job_type = T::NAME;
        sqlx::query(query)
            .bind(job)
            .bind(id.to_string())
            .bind(job_type)
            .bind(on.timestamp())
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
        let fetch_query = "SELECT * FROM Jobs WHERE id = ?1";
        let res: Option<SqlJobRequest<T>> = sqlx::query_as(fetch_query)
            .bind(job_id)
            .fetch_optional(&mut conn)
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
                    .execute(&mut tx)
                    .await
                    .map_err(|e| StorageError::Database(Box::from(e)))?;
                Ok(true)
            }
            // Worker not seen in 5 minutes yet has running jobs
            StorageWorkerPulse::RenqueueOrpharned { count } => {
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
                sqlx::query(query)
                    .bind(Utc::now().sub(chrono::Duration::minutes(5)).timestamp())
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
                "UPDATE Jobs SET status = 'Killed', done_at = strftime('%s','now') WHERE id = ?1 AND lock_by = ?2";
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
                "UPDATE Jobs SET status = 'Pending', done_at = NULL, lock_by = NULL WHERE id = ?1 AND lock_by = ?2";
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
        let query = "Select Count(*) as count from Jobs where status='Pending'";
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
                "UPDATE Jobs SET status = 'Done', done_at = strftime('%s','now') WHERE id = ?1 AND lock_by = ?2";
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
                "UPDATE Jobs SET status = 'Failed', done_at = NULL, lock_by = NULL, lock_at = NULL, run_at = ?2 WHERE id = ?1";
        sqlx::query(query)
            .bind(job_id.to_owned())
            .bind(Utc::now().add(wait).timestamp())
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
        let done_at = (*job.done_at()).map(|v| v.timestamp());
        let lock_by = job.lock_by().clone();
        let lock_at = (*job.lock_at()).map(|v| v.timestamp());
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
        let mut tx = self
            .pool
            .acquire()
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        let worker_type = T::NAME;
        let storage_name = std::any::type_name::<Self>();
        let query =
                "INSERT OR REPLACE INTO Workers (id, worker_type, storage_name, layers, last_seen) VALUES (?1, ?2, ?3, ?4, ?5);";
        sqlx::query(query)
            .bind(worker_id.to_owned())
            .bind(worker_type)
            .bind(storage_name)
            .bind(std::any::type_name::<Service>())
            .bind(Utc::now().timestamp())
            .execute(&mut tx)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(())
    }
}

#[async_trait::async_trait]

impl<J: 'static + Job + Serialize + DeserializeOwned> JobStreamExt<J> for SqliteStorage<J> {
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
                        FROM Jobs WHERE job_type = ?";
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
        let fetch_query = "SELECT * FROM Jobs WHERE status = ? AND job_type = ? ORDER BY done_at DESC, run_at DESC LIMIT 10 OFFSET ?";
        let res: Vec<SqlJobRequest<J>> = sqlx::query_as(fetch_query)
            .bind(status)
            .bind(J::NAME)
            .bind(((page - 1) * 10).to_string())
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
            "SELECT id, layers, last_seen FROM Workers WHERE worker_type = ? ORDER BY last_seen DESC LIMIT 20 OFFSET ?";
        let res: Vec<(String, String, DateTime<Utc>)> = sqlx::query_as(fetch_query)
            .bind(J::NAME)
            .bind("0")
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

    use super::*;
    use email_service::Email;

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
        assert!(sqlite.is_empty().await.is_err())
    }
}
