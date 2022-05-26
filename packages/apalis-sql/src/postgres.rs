use apalis_core::error::StorageError;
use apalis_core::job::Job;
use apalis_core::request::JobRequest;
use apalis_core::storage::{JobStream, Storage, StorageResult};
use apalis_core::worker::WorkerPulse;
use async_stream::try_stream;
use chrono::Utc;
use futures::{FutureExt, Stream};
use futures_lite::future::{self, pending, ready};
use serde::{de::DeserializeOwned, Serialize};
use sqlx::postgres::PgListener;
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
        PostgresStorage {
            pool: self.pool.clone(),
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
                  run_at timestamptz NOT NULL default now(),
                  last_error TEXT,
                  lock_at timestamptz,
                  lock_by TEXT,
                  done_at timestamptz)
        "#;
        let workers_table = r#"
                CREATE TABLE IF NOT EXISTS apalis.workers (
                    id TEXT NOT NULL,
                    worker_type TEXT NOT NULL,
                    storage_name TEXT NOT NULL,
                    last_seen timestamptz not null default now()
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
        sqlx::query("CREATE UNIQUE INDEX IF NOT EXISTS unique_job_id ON apalis.jobs (id)")
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
        sqlx::query("CREATE UNIQUE INDEX IF NOT EXISTS unique_worker_id ON  apalis.workers (id)")
            .execute(&pool)
            .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS WTIdx ON apalis.workers(worker_type)")
            .execute(&pool)
            .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS LSIdx ON apalis.workers(last_seen)")
            .execute(&pool)
            .await?;

        sqlx::query(
            r#"
            CREATE OR replace FUNCTION apalis.get_job( 
                worker_id TEXT,
                v_job_type TEXT
            ) returns apalis.jobs AS $$
            DECLARE
                v_job_id text;
                v_job_row apalis.jobs;
            BEGIN
                SELECT   id, job_type
                INTO     v_job_id, v_job_type
                FROM     apalis.jobs
                WHERE    status = 'Pending'
                AND      run_at < now()
                AND      job_type = v_job_type
                ORDER BY run_at ASC limit 1 FOR UPDATE skip LOCKED;
                
                IF v_job_id IS NULL THEN
                    RETURN NULL;
                END IF;

                UPDATE apalis.jobs
                    SET 
                        status = 'Running',      
                        lock_by = worker_id,
                        lock_at = now()
                    WHERE     id = v_job_id
                returning * INTO  v_job_row;
                RETURN v_job_row;
            END;
            $$ LANGUAGE plpgsql volatile;
            
        "#,
        )
        .execute(&pool)
        .await?;
        // sqlx::query( r#"
        //     create OR replace FUNCTION apalis.notify_new_jobs() returns trigger as $$
        //         begin
        //         perform pg_notify('apalis::job', 'insert');
        //         return new;
        //         end;
        //     $$ language plpgsql;
        //     DROP TRIGGER IF EXISTS notify_workers ON apalis.jobs;
        //     create trigger notify_workers after insert on apalis.jobs for each statement execute procedure apalis.notify_new_jobs();
        // "#)
        // .execute(&pool)
        // .await?;
        Ok(())
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
            let mut listener = PgListener::connect_with(&pool).await?;
            let _notify = listener.listen("apalis::job").await?;
            loop {
                //  Ideally wait for a job or a tick
                let interval = interval.tick().map(|_| ());
                let notification = listener.recv().map(|_| ()); // TODO: This silences errors from pubsub, needs improvement to trigger start queue
                future::race(interval, notification).await;
                let tx = pool.clone();
                let mut tx = tx.acquire().await?;
                let job_type = T::NAME;
                let fetch_query = "Select * FROM apalis.get_job($1, $2) WHERE job IS NOT NULL;";
                let job: Option<JobRequest<T>> = sqlx::query_as(fetch_query)
                    .bind(worker_id.clone())
                    .bind(job_type)
                    .fetch_optional(&mut tx)
                    .await?;
                yield job
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
        let query = "INSERT INTO apalis.jobs VALUES ($1, $2, $3, 'Pending', 0, 25, NOW() , NULL, NULL, NULL, NULL)";
        let pool = self.pool.clone();

        let fut = async move {
            let job = serde_json::to_value(&job)?;
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
            "INSERT INTO apalis.jobs VALUES ($1, $2, $3, 'Pending', 0, 25, $4, NULL, NULL, NULL, NULL)";
        let pool = self.pool.clone();
        let id = Uuid::new_v4();

        let fut = async move {
            let job = serde_json::to_value(&job)?;
            let mut pool = pool.acquire().await?;
            let job_type = T::NAME;
            sqlx::query(query)
                .bind(job)
                .bind(id.to_string())
                .bind(job_type)
                .bind(on)
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
            let fetch_query = "SELECT * FROM apalis.jobs WHERE id = $1";
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
                "UPDATE apalis.jobs SET status = 'Kill', done_at = now() WHERE id = $1 AND lock_by = $2";
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
                "UPDATE apalis.jobs SET status = 'Pending', done_at = NULL, lock_by = NULL WHERE id = $1 AND lock_by = $2";
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
                "UPDATE apalis.jobs SET status = 'Done', done_at = now() WHERE id = $1 AND lock_by = $2";
            sqlx::query(query)
                .bind(job_id.to_owned())
                .bind(worker_id.to_owned())
                .execute(&mut tx)
                .await?;
            Ok(())
        };
        Box::pin(fut)
    }

    fn reschedule(&mut self, job: &JobRequest<T>, wait: Duration) -> StorageResult<()> {
        let pool = self.pool.clone();
        let job_id = job.id();
        let fut = async move {
            let wait: i64 = wait
                .as_secs()
                .try_into()
                .map_err(|e| StorageError::Database(Box::new(e)))?;
            let wait = chrono::Duration::seconds(wait);
            let mut tx = pool.acquire().await?;
            let query =
                "UPDATE apalis.jobs SET status = 'Pending', done_at = NULL, lock_by = NULL, lock_at = NULL, run_at = $2 WHERE id = $1";
            sqlx::query(query)
                .bind(job_id.to_owned())
                .bind(Utc::now().add(wait))
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
        let done_at = job.done_at().clone();
        let lock_by = job.lock_by().clone();
        let lock_at = job.lock_at().clone();
        let last_error = job.last_error().clone();
        let fut = async move {
            let mut tx = pool.acquire().await?;
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
                "INSERT INTO apalis.workers (id, worker_type, storage_name, last_seen) VALUES ($1, $2, $3, $4) ON CONFLICT (id) 
                DO 
                   UPDATE SET last_seen = NOW()";
            sqlx::query(query)
                .bind(worker_id.to_owned())
                .bind(worker_type)
                .bind(storage_name)
                .bind(Utc::now())
                .execute(&mut tx)
                .await?;
            Ok(())
        };
        Box::pin(fut)
    }
}
