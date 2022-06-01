use apalis_core::error::JobStreamError;
use apalis_core::job::{Job, JobStreamResult};
use apalis_core::request::JobRequest;
use apalis_core::storage::StorageError;
use apalis_core::storage::StorageWorkerPulse;
use apalis_core::storage::{Storage, StorageResult};
use async_stream::try_stream;
use chrono::Utc;
use futures::Stream;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::types::Uuid;
use sqlx::{MySql, MySqlPool, Pool, Row};
use std::convert::TryInto;
use std::{marker::PhantomData, ops::Add, time::Duration};

use crate::from_row::{IntoJobRequest, SqlJobRequest};

pub struct MysqlStorage<T> {
    pool: Pool<MySql>,
    job_type: PhantomData<T>,
}

impl<T> Clone for MysqlStorage<T> {
    fn clone(&self) -> Self {
        let pool = self.pool.clone();
        MysqlStorage {
            pool,
            job_type: PhantomData,
        }
    }
}

impl<T> MysqlStorage<T> {
    pub fn new(pool: MySqlPool) -> Self {
        Self {
            pool,
            job_type: PhantomData,
        }
    }

    pub async fn connect<S: Into<String>>(db: S) -> Result<Self, sqlx::Error> {
        let pool = MySqlPool::connect(&db.into()).await?;
        Ok(Self::new(pool))
    }

    pub async fn setup(&self) -> Result<(), sqlx::Error> {
        let jobs_table = r#"
        CREATE TABLE IF NOT EXISTS jobs
            (   
                job JSON NOT NULL,
                id varchar(36) NOT NULL,
                job_type varchar(200) NOT NULL,
                status varchar(20) NOT NULL DEFAULT 'Pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 25,
                run_at datetime DEFAULT CURRENT_TIMESTAMP,
                last_error varchar(1000) DEFAULT NULL,
                lock_at datetime DEFAULT NULL,
                lock_by varchar(36) DEFAULT NULL,
                done_at datetime DEFAULT NULL,
                PRIMARY KEY (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        "#;
        let workers_table = r#"
                CREATE TABLE IF NOT EXISTS workers (
                    id varchar(36) NOT NULL,
                    worker_type  varchar(200) NOT NULL,
                    storage_name  varchar(200) NOT NULL,
                    last_seen datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        "#;
        let pool = self.pool.clone();
        sqlx::query(jobs_table).execute(&pool).await?;

        sqlx::query("CREATE INDEX job_id ON jobs(id)")
            .execute(&pool)
            .await?;
        sqlx::query("CREATE INDEX job_status ON jobs(status)")
            .execute(&pool)
            .await?;
        sqlx::query("CREATE INDEX LIdx ON jobs(lock_by)")
            .execute(&pool)
            .await?;
        sqlx::query("CREATE INDEX JTIdx ON jobs(job_type)")
            .execute(&pool)
            .await?;

        sqlx::query(workers_table).execute(&pool).await?;

        sqlx::query("CREATE INDEX Idx ON workers(id)")
            .execute(&pool)
            .await?;
        sqlx::query("CREATE INDEX WTIdx ON workers(worker_type)")
            .execute(&pool)
            .await?;
        sqlx::query("CREATE INDEX LSIdx ON workers(last_seen)")
            .execute(&pool)
            .await?;
        Ok(())
    }
}

async fn fetch_next<T>(
    pool: Pool<MySql>,
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
            let update_query = "UPDATE Jobs SET status = 'Running', lock_by = ? WHERE id = ? AND status = 'Pending' AND lock_by IS NULL;";
            sqlx::query(update_query)
                .bind(worker_id)
                .bind(job_id.clone())
                .execute(&mut tx)
                .await
                .map_err(|e| JobStreamError::BrokenPipe(Box::from(e)))?;
            let job: Option<SqlJobRequest<T>> = sqlx::query_as("Select * from jobs where id = ?")
                .bind(job_id.clone())
                .fetch_optional(&mut tx)
                .await
                .map_err(|e| JobStreamError::BrokenPipe(Box::from(e)))?;

            tx.commit()
                .await
                .map_err(|e| JobStreamError::BrokenPipe(Box::from(e)))?;

            Ok(job.as_job_request())
        }
    }
}

impl<T: DeserializeOwned + Send + Unpin + Job> MysqlStorage<T> {
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
                let fetch_query = "SELECT * FROM jobs
                    WHERE status = 'Pending' AND run_at < NOW() AND job_type = ? ORDER BY run_at ASC LIMIT 1 FOR UPDATE";
                let job: Option<SqlJobRequest<T>> = sqlx::query_as(fetch_query)
                    .bind(job_type)
                    .fetch_optional(&mut tx)
                    .await.map_err(|e| JobStreamError::BrokenPipe(Box::from(e)))?;
                yield fetch_next(pool.clone(), worker_id.clone(), job.as_job_request()).await.map_err(|e| JobStreamError::BrokenPipe(Box::from(e)))?
            }
        }
    }
}

#[async_trait::async_trait]
impl<T> Storage for MysqlStorage<T>
where
    T: Job + Serialize + DeserializeOwned + Send + 'static + Unpin,
{
    type Output = T;

    async fn push(&mut self, job: Self::Output) -> StorageResult<()> {
        let id = Uuid::new_v4();
        let query =
            "INSERT INTO jobs VALUES (?, ?, ?, 'Pending', 0, 25, now(), NULL, NULL, NULL, NULL)";
        let pool = self.pool.clone();

        let job = serde_json::to_string(&job)?;
        let mut pool = pool
            .acquire()
            .await
            .map_err(|e| StorageError::Connection(Box::from(e)))?;
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
            "INSERT INTO jobs VALUES (?, ?, ?, 'Pending', 0, 25, ?, NULL, NULL, NULL, NULL)";
        let pool = self.pool.clone();
        let id = Uuid::new_v4();

        let job = serde_json::to_string(&job)?;
        let mut pool = pool
            .acquire()
            .await
            .map_err(|e| StorageError::Connection(Box::from(e)))?;
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
            .map_err(|e| StorageError::Connection(Box::from(e)))?;
        let fetch_query = "SELECT * FROM jobs WHERE id = ?";
        let res: Option<SqlJobRequest<T>> = sqlx::query_as(fetch_query)
            .bind(job_id)
            .fetch_optional(&mut conn)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(res.as_job_request())
    }

    async fn heartbeat(&mut self, pulse: StorageWorkerPulse) -> StorageResult<bool> {
        // let pool = self.pool.clone();

        match pulse {
            StorageWorkerPulse::EnqueueScheduled { count: _ } => {
                // Idealy jobs are queue via run_at. So this is not necessary
                Ok(true)
            }
            // Worker not seen in 5 minutes yet has running jobs
            StorageWorkerPulse::RenqueueOrpharned { count: _ } => {
                // let job_type = T::NAME;
                // let mut tx = pool.acquire().await?;
                // let query = r#"Update jobs
                //         SET status = "Pending", done_at = NULL, lock_by = NULL, lock_at = NULL, last_error ="Job was abandoned"
                //         WHERE id IN
                //             (SELECT jobs.id from Jobs INNER join workers ON lock_by = workers.id
                //                 WHERE status= "Running" AND workers.last_seen < ?
                //                 AND workers.worker_type = ? ORDER BY lock_at ASC) LIMIT ?;"#;
                // sqlx::query(query)
                //     .bind(Utc::now().sub(chrono::Duration::minutes(5)))
                //     .bind(job_type)
                //     .bind(count)
                //     .execute(&mut tx)
                //     .await?;
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
            .map_err(|e| StorageError::Connection(Box::from(e)))?;
        let query = "UPDATE Jobs SET status = 'Kill', done_at = NOW() WHERE id = ? AND lock_by = ?";
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
                "UPDATE Jobs SET status = 'Pending', done_at = NULL, lock_by = NULL WHERE id = ? AND lock_by = ?";
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
        let query = "UPDATE Jobs SET status = 'Done', done_at = now() WHERE id = ? AND lock_by = ?";
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
            .map_err(|e| StorageError::Connection(Box::from(e)))?;
        let query =
                "UPDATE Jobs SET status = 'Pending', done_at = NULL, lock_by = NULL, lock_at = NULL, run_at = ? WHERE id = ?";
        sqlx::query(query)
            .bind(Utc::now().add(wait))
            .bind(job_id.to_owned())
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
        let done_at = job.done_at().clone();
        let lock_by = job.lock_by().clone();
        let lock_at = job.lock_at().clone();
        let last_error = job.last_error().clone();

        let mut tx = pool
            .acquire()
            .await
            .map_err(|e| StorageError::Connection(Box::from(e)))?;
        let query =
                "UPDATE jobs SET status = ?, attempts = ?, done_at = ?, lock_by = ?, lock_at = ?, last_error = ? WHERE id = ?";
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

    async fn keep_alive(&mut self, worker_id: String) -> StorageResult<()> {
        let pool = self.pool.clone();

        let mut tx = pool
            .acquire()
            .await
            .map_err(|e| StorageError::Connection(Box::from(e)))?;
        let worker_type = T::NAME;
        let storage_name = std::any::type_name::<Self>();
        let query =
            "REPLACE INTO workers (id, worker_type, storage_name, last_seen) VALUES (?, ?, ?, ?);";
        sqlx::query(query)
            .bind(worker_id.to_owned())
            .bind(worker_type)
            .bind(storage_name)
            .bind(Utc::now())
            .execute(&mut tx)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(())
    }
}
