use apalis_core::request::JobRequest;
use apalis_core::storage::{Storage, StorageResult};
use chrono::Utc;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::{Pool, Sqlite, SqlitePool};
use std::{fmt::Debug, marker::PhantomData, ops::Add, time::Duration};

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
    pub async fn new<S: Into<String>>(db: S) -> Result<Self, sqlx::Error> {
        let pool = SqlitePool::connect(&db.into()).await?;
        Ok(Self {
            pool,
            job_type: PhantomData,
        })
    }

    pub async fn setup(&self) {
        let query = r#"
        CREATE TABLE IF NOT EXISTS Jobs
                ( job TEXT NOT NULL,
                  id TEXT NOT NULL,
                  status TEXT NOT NULL DEFAULT 'Pending',
                  attempts INTEGER NOT NULL DEFAULT 0,
                  max_attempts INTEGER NOT NULL DEFAULT 25,
                  run_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
                  last_error TEXT,
                  lock_at INTEGER,
                  lock_by TEXT,
                  done_at INTEGER )
        "#;
        let pool = self.pool.clone();
        sqlx::query(query)
            .execute(&pool)
            .await
            .expect("Failed to BEGIN transaction.");

        sqlx::query("CREATE INDEX IF NOT EXISTS TIdx ON Jobs(id)")
            .execute(&pool)
            .await
            .expect("Failed to BEGIN transaction.");
        sqlx::query("CREATE INDEX IF NOT EXISTS SIdx ON Jobs(status)")
            .execute(&pool)
            .await
            .expect("Failed to BEGIN transaction.");

        sqlx::query("PRAGMA journal_mode = 'WAL';")
            .execute(&pool)
            .await
            .expect("Failed to BEGIN transaction.");
        sqlx::query("PRAGMA temp_store = 2;")
            .execute(&pool)
            .await
            .expect("Failed to BEGIN transaction.");
        sqlx::query("PRAGMA synchronous = 1;")
            .execute(&pool)
            .await
            .expect("Failed to BEGIN transaction.");
        // sqlx::query("PRAGMA cache_size = 64_000;")
        //     .execute(&pool)
        //     .await
        //     .expect("Failed to BEGIN transaction.");
    }
}

impl<T> Storage for SqliteStorage<T>
where
    T: Serialize + DeserializeOwned + Send + 'static + Unpin,
{
    type Output = T;

    fn push(&mut self, job: Self::Output) -> StorageResult<()> {
        let query = "INSERT INTO Jobs VALUES (?1, lower(hex(randomblob(16))), 'Pending', 0, 25, strftime('%s','now'), NULL, NULL, NULL, NULL)";
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
            let fetch_query = "SELECT * FROM Jobs
            WHERE rowid = (SELECT min(rowid) FROM Jobs
                           WHERE status = 'Pending')";
            let job: Option<JobRequest<T>> =
                sqlx::query_as(fetch_query).fetch_optional(&mut tx).await?;
            if job.is_none() {
                return Ok(None);
            }
            let job = job.unwrap();
            let job_id = job.id();
            let update_query = "UPDATE Jobs SET status = 'Running', attempts = attempts + 1,  lock_at = strftime('%s','now') WHERE id = ?1 AND status = 'Pending'";
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
