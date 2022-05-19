use apalis_core::error::StorageError;
use apalis_core::job::Job;
use apalis_core::request::JobRequest;
use apalis_core::storage::{JobStream, Storage, StorageResult};
use apalis_core::worker::WorkerPulse;
use async_stream::try_stream;
use chrono::Utc;
use futures::Stream;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::types::Uuid;
use sqlx::{PgPool, Pool, Postgres, Row};
use std::convert::TryInto;
use std::ops::Sub;
use std::{marker::PhantomData, ops::Add, time::Duration};

pub struct PostgresStorage<T> {
    pool: PgPool,
    job_type: PhantomData<T>,
}

impl<T> Clone for PostgresStorage<T> {
    fn clone(&self) -> Self {
        let pool = self.pool.clone();
        PostgresStorage {
            pool,
            job_type: PhantomData,
        }
    }
}

impl<T> PostgresStorage<T> {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            job_type: PhantomData,
        }
    }

    pub async fn connect<S: Into<String>>(db: S) -> Result<Self, sqlx::Error> {
        let pool = PgPool::connect(&db.into()).await?;
        Ok(Self::new(pool))
    }

    pub async fn setup(&self) -> Result<(), sqlx::Error> {
        let jobs_table = r#"
        CREATE TABLE IF NOT EXISTS apalis.jobs
                ( job JSONB NOT NULL,
                  id TEXT NOT NULL,
                  job_type TEXT NOT NULL,
                  status TEXT NOT NULL DEFAULT 'Pending',
                  attempts INTEGER NOT NULL DEFAULT 0,
                  max_attempts INTEGER NOT NULL DEFAULT 25,
                  run_at timestamp without time zone NOT NULL default (now() at time zone 'utc'),
                  last_error TEXT,
                  lock_at timestamp without time zone,
                  lock_by TEXT,
                  done_at timestamp without time zone)
        "#;
        let workers_table = r#"
                CREATE TABLE IF NOT EXISTS apalis.workers (
                    id TEXT NOT NULL,
                    worker_type TEXT NOT NULL,
                    storage_name TEXT NOT NULL,
                    last_seen timestamp without time zone NOT NULL default (now() at time zone 'utc')
                )
        "#;
        let pool = self.pool.clone();
        sqlx::query(jobs_table).execute(&pool).await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS TIdx ON apalis.jobs(id)")
            .execute(&pool)
            .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS SIdx ON apalis.jobs(status)")
            .execute(&pool)
            .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS LIdx ON apalis.jobs(lock_by)")
            .execute(&pool)
            .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS JTIdx ON apalis.jobs(job_type)")
            .execute(&pool)
            .await?;

        sqlx::query(workers_table).execute(&pool).await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS Idx ON apalis.workers(id)")
            .execute(&pool)
            .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS WTIdx ON apalis.workers(worker_type)")
            .execute(&pool)
            .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS LSIdx ON apalis.workers(last_seen)")
            .execute(&pool)
            .await?;
        Ok(())
    }
}

async fn fetch_next<T>(
    pool: PgPool,
    worker_id: String,
    job: Option<JobRequest<T>>,
) -> Result<Option<JobRequest<T>>, StorageError>
where
    T: Send + Unpin + DeserializeOwned,
{
    match job {
        None => Ok(None),
        Some(job) => {
            let mut tx = pool.begin().await?;
            let job_id = job.id();
            let update_query = "UPDATE apalis.jobs SET status = 'Running', lock_by = ?2 WHERE id = ?1 AND status = 'Pending' AND lock_by IS NULL; Select * from apalis.jobs where id = ?1 AND lock_by = ?2";
            let job: Option<JobRequest<T>> = sqlx::query_as(update_query)
                .bind(job_id.clone())
                .bind(worker_id)
                .fetch_optional(&mut tx)
                .await?;
            tx.commit().await?;
            Ok(job)
        }
    }
}

impl<T: DeserializeOwned + Send + Unpin + Job> PostgresStorage<T> {
    fn stream_jobs(
        &self,
        worker_id: String,
        interval: Duration,
    ) -> impl Stream<Item = Result<Option<JobRequest<T>>, StorageError>> {
        let pool = self.pool.clone();
        let mut interval = tokio::time::interval(interval);
        try_stream! {
            loop {
                interval.tick().await;
                let tx = pool.clone();
                let mut tx = tx.acquire().await?;
                let job_type = T::NAME;
                let fetch_query = "SELECT * FROM apalis.jobs
                    WHERE rowid = (SELECT min(rowid) FROM apalis.jobs
                    WHERE status = 'Pending' AND run_at < ?1 AND job_type = ?2)";
                let job: Option<JobRequest<T>> = sqlx::query_as(fetch_query)
                    .bind(Utc::now().timestamp())
                    .bind(job_type)
                    .fetch_optional(&mut tx)
                    .await?;
                yield fetch_next(pool.clone(), worker_id.clone(), job).await?
            }
        }
    }
}

impl<T> Storage for PostgresStorage<T>
where
    T: Job + Serialize + DeserializeOwned + Send + 'static + Unpin,
{
    type Output = T;

    fn push(&mut self, job: Self::Output) -> StorageResult<()> {
        let id = Uuid::new_v4();
        let query = "INSERT INTO apalis.jobs VALUES (?1, ?2, ?3, 'Pending', 0, 25, strftime('%s','now'), NULL, NULL, NULL, NULL)";
        let pool = self.pool.clone();

        let fut = async move {
            let job = serde_json::to_string(&job)?;
            let mut pool = pool.acquire().await?;
            let job_type = T::NAME;
            sqlx::query(query)
                .bind(job)
                .bind(id.to_string())
                .bind(job_type.to_string())
                .execute(&mut pool)
                .await?;
            Ok(())
        };
        Box::pin(fut)
    }

    fn schedule(&mut self, job: Self::Output, on: chrono::DateTime<Utc>) -> StorageResult<()> {
        let query =
            "INSERT INTO apalis.jobs VALUES (?1, ?2, ?3, 'Pending', 0, 25, ?4, NULL, NULL, NULL, NULL)";
        let pool = self.pool.clone();
        let id = Uuid::new_v4();

        let fut = async move {
            let job = serde_json::to_string(&job)?;
            let mut pool = pool.acquire().await?;
            let job_type = T::NAME;
            sqlx::query(query)
                .bind(job)
                .bind(id.to_string())
                .bind(job_type)
                .bind(on.timestamp())
                .execute(&mut pool)
                .await?;
            Ok(())
        };
        Box::pin(fut)
    }

    fn fetch_by_id(&self, job_id: String) -> StorageResult<Option<JobRequest<Self::Output>>> {
        let pool = self.pool.clone();
        let fut = async move {
            let mut conn = pool.clone().acquire().await?;
            let fetch_query = "SELECT * FROM apalis.jobs WHERE id = ?1";
            Ok(sqlx::query_as(fetch_query)
                .bind(job_id)
                .fetch_optional(&mut conn)
                .await?)
        };
        Box::pin(fut)
    }

    fn heartbeat(&mut self, pulse: WorkerPulse) -> StorageResult<bool> {
        let pool = self.pool.clone();

        let fut = async move {
            match pulse {
                WorkerPulse::EnqueueScheduled { count: _ } => {
                    // Idealy jobs are queue via run_at. So this is not necessary
                    Ok(true)
                }
                // Worker not seen in 5 minutes yet has running jobs
                WorkerPulse::RenqueueOrpharned { count } => {
                    let job_type = T::NAME;
                    let mut tx = pool.acquire().await?;
                    let query = r#"Update apalis.jobs 
                            SET status = "Pending", done_at = NULL, lock_by = NULL, lock_at = NULL, last_error ="Job was abandoned"
                            WHERE id in 
                                (SELECT jobs.id from apalis.jobs INNER join apalis.workers ON lock_by = workers.id 
                                    WHERE status= "Running" AND workers.last_seen < ?1
                                    AND workers.worker_type = ?2 ORDER BY lock_at ASC LIMIT ?3);"#;
                    sqlx::query(query)
                        .bind(Utc::now().sub(chrono::Duration::minutes(5)).timestamp())
                        .bind(job_type)
                        .bind(count)
                        .execute(&mut tx)
                        .await?;
                    Ok(true)
                }
                _ => todo!(),
            }
        };
        Box::pin(fut)
    }

    fn kill(&mut self, worker_id: String, job_id: String) -> StorageResult<()> {
        let pool = self.pool.clone();
        let fut = async move {
            let mut tx = pool.acquire().await?;
            let query =
                "UPDATE apalis.jobs SET status = 'Kill', done_at = strftime('%s','now') WHERE id = ?1 AND lock_by = ?2";
            sqlx::query(query)
                .bind(job_id.to_owned())
                .bind(worker_id.to_owned())
                .execute(&mut tx)
                .await?;
            Ok(())
        };
        Box::pin(fut)
    }

    /// Puts the job instantly back into the queue
    /// Another [Worker] may consume
    fn retry(&mut self, worker_id: String, job_id: String) -> StorageResult<()> {
        let pool = self.pool.clone();
        let fut = async move {
            let mut tx = pool.acquire().await?;
            let query =
                "UPDATE apalis.jobs SET status = 'Pending', done_at = NULL, lock_by = NULL WHERE id = ?1 AND lock_by = ?2";
            sqlx::query(query)
                .bind(job_id.to_owned())
                .bind(worker_id.to_owned())
                .execute(&mut tx)
                .await?;
            Ok(())
        };
        Box::pin(fut)
    }

    fn consume(&mut self, worker_id: String, interval: Duration) -> JobStream<T> {
        Box::pin(self.stream_jobs(worker_id, interval))
    }
    fn len(&self) -> StorageResult<i64> {
        let pool = self.pool.clone();
        let fut = async move {
            let mut tx = pool.acquire().await?;
            let query = "Select Count(*) as count from apalis.jobs where status='Pending'";
            let record = sqlx::query(query).fetch_one(&mut tx).await?;
            Ok(record.try_get("count")?)
        };
        Box::pin(fut)
    }
    fn ack(&mut self, worker_id: String, job_id: String) -> StorageResult<()> {
        let pool = self.pool.clone();
        let fut = async move {
            let mut tx = pool.acquire().await?;
            let query =
                "UPDATE apalis.jobs SET status = 'Done', done_at = strftime('%s','now') WHERE id = ?1 AND lock_by = ?2";
            sqlx::query(query)
                .bind(job_id.to_owned())
                .bind(worker_id.to_owned())
                .execute(&mut tx)
                .await?;
            Ok(())
        };
        Box::pin(fut)
    }

    fn reschedule(&mut self, job_id: String, wait: Duration) -> StorageResult<()> {
        let pool = self.pool.clone();
        let fut = async move {
            let wait: i64 = wait
                .as_secs()
                .try_into()
                .map_err(|e| StorageError::Database(Box::new(e)))?;
            let wait = chrono::Duration::seconds(wait);
            let mut tx = pool.acquire().await?;
            let query =
                "UPDATE apalis.jobs SET status = 'Pending', done_at = NULL, lock_by = NULL, lock_at = NULL, run_at = ?2 WHERE id = ?1";
            sqlx::query(query)
                .bind(job_id.to_owned())
                .bind(Utc::now().add(wait).timestamp())
                .execute(&mut tx)
                .await?;
            Ok(())
        };
        Box::pin(fut)
    }

    fn update_by_id(&self, job_id: String, job: &JobRequest<Self::Output>) -> StorageResult<()> {
        let pool = self.pool.clone();
        let status = job.status().as_ref().to_string();
        let attempts = job.attempts();
        let done_at = job.done_at().clone().map(|v| v.timestamp());
        let lock_by = job.lock_by().clone();
        let lock_at = job.lock_at().clone().map(|v| v.timestamp());
        let last_error = job.last_error().clone();
        let fut = async move {
            let mut tx = pool.acquire().await?;
            let query =
                "UPDATE apalis.jobs SET status = ?1, attempts = ?2, done_at = ?3, lock_by = ?4, lock_at = ?5, last_error = ?6 WHERE id = ?7";
            sqlx::query(query)
                .bind(status.to_owned())
                .bind(attempts)
                .bind(done_at)
                .bind(lock_by)
                .bind(lock_at)
                .bind(last_error)
                .bind(job_id.to_owned())
                .execute(&mut tx)
                .await?;
            Ok(())
        };
        Box::pin(fut)
    }

    fn keep_alive(&mut self, worker_id: String) -> StorageResult<()> {
        let pool = self.pool.clone();
        let fut = async move {
            let mut tx = pool.acquire().await?;
            let worker_type = T::NAME;
            let storage_name = std::any::type_name::<Self>();
            let query =
                "INSERT OR REPLACE INTO apalis.workers (id, worker_type, storage_name, last_seen) VALUES (?1, ?2, ?3, ?4);";
            sqlx::query(query)
                .bind(worker_id.to_owned())
                .bind(worker_type)
                .bind(storage_name)
                .bind(Utc::now().timestamp())
                .execute(&mut tx)
                .await?;
            Ok(())
        };
        Box::pin(fut)
    }
}
