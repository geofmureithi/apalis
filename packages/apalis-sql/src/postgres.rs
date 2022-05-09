use apalis_core::request::JobRequest;
use apalis_core::storage::{Storage, StorageResult};
use chrono::Utc;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::{Pool, Postgres, PostgresPool};
use std::{fmt::Debug, marker::PhantomData, ops::Add, time::Duration};

pub struct PostgresStorage<T> {
    pool: Pool<Postgres>,
    job_type: PhantomData<T>,
    schema: String,
}

impl<T> Clone for PostgresStorage<T> {
    fn clone(&self) -> Self {
        let pool = self.pool.clone();
        PostgresStorage {
            pool,
            job_type: PhantomData,
            schema: self.schema.clone(),
        }
    }
}

impl<T> PostgresStorage<T> {
    pub async fn new<S: Into<String>>(db: S) -> Result<Self, sqlx::Error> {
        let pool = PostgresPool::connect(&db.into()).await?;
        Ok(Self {
            pool,
            job_type: PhantomData,
            schema: "apalis".to_string(),
        })
    }

    pub async fn setup(&self) -> Result<(), sqlx::Error> {
        let query = r#"
        CREATE TABLE IF NOT EXISTS apalis.jobs
                ( id SERIAL PRIMARY KEY,
                  job JSONB NOT NULL,
                  queue_name TEXT NOT NULL,
                  status VARCHAR NOT NULL DEFAULT 'Pending',
                  attempts INTEGER NOT NULL DEFAULT 0,
                  max_attempts INTEGER NOT NULL DEFAULT 25,
                  run_at TIMESTAMP NOT NULL DEFAULT NOW(),
                  last_error TEXT,
                  lock_at INTEGER,
                  lock_by TEXT,
                  done_at TIMESTAMP))
        "#;
        let pool = self.pool.clone();
        sqlx::query(query).execute(&pool).await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS TIdx ON apalis.jobs(id);")
            .execute(&pool)
            .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS SIdx ON apalis.jobs(status)")
            .execute(&pool)
            .await?
    }
}

impl<T> Storage for PostgresStorage<T>
where
    T: Serialize + DeserializeOwned + Send + 'static + Unpin,
{
    type Output = T;

    fn push(&mut self, job: Self::Output) -> StorageResult<()> {
        let query = "INSERT INTO apalis.jobs VALUES (?1, lower(hex(randomblob(16))), 'Pending', 0, 25, strftime('%s','now'), NULL, NULL, NULL, NULL)";
        let pool = self.pool.clone();
        let job = serde_json::to_string(&job).unwrap();
        let fut = async move {
            let mut pool = pool.acquire().await?;
            sqlx::query(query).bind(job).execute(&mut pool).await?;
            Ok(())
        };
        Box::pin(fut)
    }

    fn consume(&mut self) -> StorageResult<Option<JobRequest<Self::Output>>> {
        let pool = self.pool.clone();
        let fut = async move {
            let mut tx = pool.begin().await.unwrap();
            let fetch_query = "SELECT * FROM apalis.jobs
            WHERE rowid = (SELECT min(rowid) FROM apalis.jobs
                           WHERE status = 'Pending')";
            let job: Option<JobRequest<T>> =
                sqlx::query_as(fetch_query).fetch_optional(&mut tx).await?;
            if job.is_none() {
                return Ok(None);
            }
            let job = job.unwrap();
            let job_id = job.id();
            let update_query = "UPDATE apalis.jobs SET status = 'Running', attempts = attempts + 1, lock_at = NOW() WHERE id = ?1 AND status = 'Pending'";
            sqlx::query(update_query)
                .bind(job_id.to_owned())
                .execute(&mut tx)
                .await?;
            tx.commit().await?;
            Ok(Some(job))
        };
        Box::pin(fut)
    }
    fn ack(&mut self, job_id: String) -> StorageResult<()> {
        let pool = self.pool.clone();
        let fut = async move {
            let mut tx = pool.begin().await?;
            let query =
                "UPDATE Jobs SET status = 'Done', done_at = strftime('%s','now') WHERE id = ?1";
            sqlx::query(query)
                .bind(job_id.to_owned())
                .execute(&mut tx)
                .await?;
            Ok(tx.commit().await?)
        };
        Box::pin(fut)
    }
    fn reschedule(&mut self, job_id: String, wait: chrono::Duration) -> StorageResult<()> {
        let pool = self.pool.clone();
        let fut = async move {
            let mut tx = pool.begin().await?;
            let query =
                "UPDATE Jobs SET status = 'Pending', attempts = attempts + 1, done_at = NULL, lock_by = NULL, lock_at = NULL, run_at = ?2 WHERE id = ?1";
            sqlx::query(query)
                .bind(job_id.to_owned())
                .bind(Utc::now().add(wait).timestamp())
                .execute(&mut tx)
                .await?;
            Ok(tx.commit().await?)
        };
        Box::pin(fut)
    }
}
