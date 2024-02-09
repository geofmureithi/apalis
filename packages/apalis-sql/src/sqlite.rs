use crate::context::SqlContext;
use crate::Config;

use apalis_core::codec::json::JsonCodec;
use apalis_core::error::Error;
use apalis_core::layers::{Ack, AckLayer};
use apalis_core::poller::controller::Controller;
use apalis_core::poller::stream::BackendStream;
use apalis_core::poller::Poller;
use apalis_core::request::{Request, RequestStream};
use apalis_core::storage::{Job, Storage};
use apalis_core::task::task_id::TaskId;
use apalis_core::worker::WorkerId;
use apalis_core::{Backend, Codec};
use async_stream::try_stream;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};
use serde::{de::DeserializeOwned, Serialize};
use sqlx::{Pool, Row, Sqlite};
use std::convert::TryInto;
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;
use std::{marker::PhantomData, time::Duration};

use crate::from_row::SqlRequest;

pub use sqlx::sqlite::SqlitePool;
/// Represents a [Storage] that persists to Sqlite
// #[derive(Debug)]
pub struct SqliteStorage<T> {
    pool: Pool<Sqlite>,
    job_type: PhantomData<T>,
    controller: Controller,
    config: Config,
    codec: Arc<Box<dyn Codec<T, String, Error = Error> + Sync + Send + 'static>>,
}

impl<T> fmt::Debug for SqliteStorage<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MysqlStorage")
            .field("pool", &self.pool)
            .field("job_type", &"PhantomData<T>")
            .field("controller", &self.controller)
            .field("config", &self.config)
            .field(
                "codec",
                &"Arc<Box<dyn Codec<T, String, Error = Error> + Sync + Send + 'static>>",
            )
            // .field("ack_notify", &self.ack_notify)
            .finish()
    }
}

impl<T> Clone for SqliteStorage<T> {
    fn clone(&self) -> Self {
        let pool = self.pool.clone();
        SqliteStorage {
            pool,
            job_type: PhantomData,
            controller: self.controller.clone(),
            config: self.config.clone(),
            codec: self.codec.clone(),
        }
    }
}

impl SqliteStorage<()> {
    /// Perform migrations for storage
    #[cfg(feature = "migrate")]
    pub async fn setup(pool: &Pool<Sqlite>) -> Result<(), sqlx::Error> {
        sqlx::query("PRAGMA journal_mode = 'WAL';")
            .execute(pool)
            .await?;
        sqlx::query("PRAGMA temp_store = 2;").execute(pool).await?;
        sqlx::query("PRAGMA synchronous = NORMAL;")
            .execute(pool)
            .await?;
        sqlx::query("PRAGMA cache_size = 64000;")
            .execute(pool)
            .await?;
        Self::migrations().run(pool).await?;
        Ok(())
    }

    /// Get sqlite migrations without running them
    #[cfg(feature = "migrate")]
    pub fn migrations() -> sqlx::migrate::Migrator {
        sqlx::migrate!("migrations/sqlite")
    }
}

impl<T: Job + Serialize + DeserializeOwned> SqliteStorage<T> {
    /// Construct a new Storage from a pool
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            job_type: PhantomData,
            controller: Controller::new(),
            config: Config::default(),
            codec: Arc::new(Box::new(JsonCodec)),
        }
    }
    /// Keeps a storage notified that the worker is still alive manually
    pub async fn keep_alive_at<Service>(
        &mut self,
        worker_id: &WorkerId,
        last_seen: i64,
    ) -> Result<(), sqlx::Error> {
        let pool = self.pool.clone();
        let worker_type = T::NAME;
        let storage_name = std::any::type_name::<Self>();
        let query = "INSERT INTO Workers (id, worker_type, storage_name, layers, last_seen)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (id) DO
                   UPDATE SET last_seen = EXCLUDED.last_seen";
        sqlx::query(query)
            .bind(worker_id.to_string())
            .bind(worker_type)
            .bind(storage_name)
            .bind(std::any::type_name::<Service>())
            .bind(last_seen)
            .execute(&pool)
            .await?;
        Ok(())
    }

    /// Expose the pool for other functionality, eg custom migrations
    pub fn pool(&self) -> &Pool<Sqlite> {
        &self.pool
    }
}

async fn fetch_next<T: Job>(
    pool: Pool<Sqlite>,
    worker_id: &WorkerId,
    id: String,
) -> Result<Option<SqlRequest<String>>, sqlx::Error> {
    let now: i64 = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .try_into()
        .unwrap();
    let update_query = "UPDATE Jobs SET status = 'Running', lock_by = ?2, lock_at = ?3 WHERE id = ?1 AND job_type = ?4 AND status = 'Pending' AND lock_by IS NULL; Select * from Jobs where id = ?1 AND lock_by = ?2 AND job_type = ?4";
    let job: Option<SqlRequest<String>> = sqlx::query_as(update_query)
        .bind(id.to_string())
        .bind(worker_id.to_string())
        .bind(now)
        .bind(T::NAME)
        .fetch_optional(&pool)
        .await?;

    Ok(job)
}

impl<T: DeserializeOwned + Send + Unpin + Job> SqliteStorage<T> {
    fn stream_jobs(
        &self,
        worker_id: &WorkerId,
        interval: Duration,
        buffer_size: usize,
    ) -> impl Stream<Item = Result<Option<Request<T>>, sqlx::Error>> {
        let pool = self.pool.clone();
        let worker_id = worker_id.clone();
        let codec = self.codec.clone();
        try_stream! {
            loop {
                apalis_core::sleep(interval).await;
                let tx = pool.clone();
                let mut tx = tx.acquire().await?;
                let job_type = T::NAME;
                let fetch_query = "SELECT id FROM Jobs
                    WHERE (status = 'Pending' OR (status = 'Failed' AND attempts < max_attempts)) AND run_at < ?1 AND job_type = ?2 LIMIT ?3";
                let now: i64 = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs().try_into().unwrap();
                let ids: Vec<(String,)> = sqlx::query_as(fetch_query)
                    .bind(now)
                    .bind(job_type)
                    .bind(i64::try_from(buffer_size).unwrap())
                    .fetch_all(&mut *tx)
                    .await?;
                for id in ids {
                    yield fetch_next::<T>(pool.clone(), &worker_id, id.0).await?.map(|c| SqlRequest {
                        context: c.context,
                        req: codec.decode(&c.req).unwrap(),
                    })
                    .map(Into::into);
                }
            }
        }
    }
}

impl<T> Storage for SqliteStorage<T>
where
    T: Job + Serialize + DeserializeOwned + Send + 'static + Unpin + Sync,
{
    type Job = T;

    type Error = sqlx::Error;

    type Identifier = TaskId;

    async fn push(&mut self, job: Self::Job) -> Result<TaskId, Self::Error> {
        let id = TaskId::new();
        let query = "INSERT INTO Jobs VALUES (?1, ?2, ?3, 'Pending', 0, 25, strftime('%s','now'), NULL, NULL, NULL, NULL)";
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

    async fn schedule(&mut self, job: Self::Job, on: i64) -> Result<TaskId, Self::Error> {
        let query =
            "INSERT INTO Jobs VALUES (?1, ?2, ?3, 'Pending', 0, 25, ?4, NULL, NULL, NULL, NULL)";
        let pool = self.pool.clone();
        let id = TaskId::new();
        let job = serde_json::to_string(&job).unwrap();
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
    ) -> Result<Option<Request<Self::Job>>, Self::Error> {
        let pool = self.pool.clone();
        let fetch_query = "SELECT * FROM Jobs WHERE id = ?1";
        let res: Option<SqlRequest<String>> = sqlx::query_as(fetch_query)
            .bind(job_id.to_string())
            .fetch_optional(&pool)
            .await?;
        Ok(res
            .map(|c| SqlRequest {
                context: c.context,
                req: self.codec.decode(&c.req).unwrap(),
            })
            .map(Into::into))
    }

    // /// Used for scheduling jobs via [StorageWorkerPulse] signals
    // async fn heartbeat(&mut self, pulse: StorageWorkerPulse) -> Result<bool, sqlx::Error> {
    //     let pool = self.pool.clone();

    //     match pulse {
    //         StorageWorkerPulse::EnqueueScheduled { count } => {

    //             Ok(true)
    //         }
    //         // Worker not seen in 5 minutes yet has running jobs
    //         StorageWorkerPulse::ReenqueueOrphaned {
    //             count,
    //             timeout_worker,
    //         } => {

    //             Ok(true)
    //         }
    //         _ => todo!(),
    //     }
    // }

    async fn len(&self) -> Result<i64, Self::Error> {
        let pool = self.pool.clone();

        let query = "Select Count(*) as count from Jobs where status='Pending'";
        let record = sqlx::query(query).fetch_one(&pool).await?;
        record.try_get("count")
    }

    async fn reschedule(&mut self, job: Request<T>, wait: Duration) -> Result<(), Self::Error> {
        let pool = self.pool.clone();
        let task_id = job.get::<TaskId>().unwrap();

        let wait: i64 = wait.as_secs().try_into().unwrap();

        let mut tx = pool.acquire().await?;
        let query =
                "UPDATE Jobs SET status = 'Failed', done_at = NULL, lock_by = NULL, lock_at = NULL, run_at = ?2 WHERE id = ?1";
        let now: i64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .try_into()
            .unwrap();
        let wait_until = now + wait;

        sqlx::query(query)
            .bind(task_id.to_string())
            .bind(wait_until)
            .execute(&mut *tx)
            .await?;
        Ok(())
    }

    async fn update(&self, job: Request<Self::Job>) -> Result<(), Self::Error> {
        let pool = self.pool.clone();
        let ctx = job.get::<SqlContext>().unwrap();
        let status = ctx.status().to_string();
        let attempts = ctx.attempts();
        let done_at = *ctx.done_at();
        let lock_by = ctx.lock_by().clone();
        let lock_at = *ctx.lock_at();
        let last_error = ctx.last_error().clone();
        let job_id = ctx.id();
        let mut tx = pool.acquire().await?;
        let query =
                "UPDATE Jobs SET status = ?1, attempts = ?2, done_at = ?3, lock_by = ?4, lock_at = ?5, last_error = ?6 WHERE id = ?7";
        sqlx::query(query)
            .bind(status.to_owned())
            .bind::<i64>(attempts.current().try_into().unwrap())
            .bind(done_at)
            .bind(lock_by.map(|w| w.name().to_string()))
            .bind(lock_at)
            .bind(last_error)
            .bind(job_id.to_string())
            .execute(&mut *tx)
            .await?;
        Ok(())
    }

    async fn is_empty(&self) -> Result<bool, Self::Error> {
        todo!()
    }
}

impl<T> SqliteStorage<T> {
    /// Puts the job instantly back into the queue
    /// Another [Worker] may consume
    pub async fn retry(
        &mut self,
        worker_id: &WorkerId,
        job_id: &TaskId,
    ) -> Result<(), sqlx::Error> {
        let pool = self.pool.clone();

        let mut tx = pool.acquire().await?;
        let query =
                "UPDATE Jobs SET status = 'Pending', done_at = NULL, lock_by = NULL WHERE id = ?1 AND lock_by = ?2";
        sqlx::query(query)
            .bind(job_id.to_string())
            .bind(worker_id.to_string())
            .execute(&mut *tx)
            .await?;
        Ok(())
    }

    /// Kill a job
    pub async fn kill(&mut self, worker_id: &WorkerId, job_id: &TaskId) -> Result<(), sqlx::Error> {
        let pool = self.pool.clone();

        let mut tx = pool.begin().await?;
        let query =
                "UPDATE Jobs SET status = 'Killed', done_at = strftime('%s','now') WHERE id = ?1 AND lock_by = ?2";
        sqlx::query(query)
            .bind(job_id.to_string())
            .bind(worker_id.to_string())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Add jobs that failed back to the queue if there are still remaining attemps
    pub async fn reenqueue_failed(&self) -> Result<(), sqlx::Error>
    where
        T: Job,
    {
        let job_type = T::NAME;
        let mut tx = self.pool.acquire().await?;
        let query = r#"Update Jobs
                            SET status = "Pending", done_at = NULL, lock_by = NULL, lock_at = NULL
                            WHERE id in
                                (SELECT Jobs.id from Jobs
                                    WHERE status= "Failed" AND Jobs.attempts < Jobs.max_attempts
                                     ORDER BY lock_at ASC LIMIT ?2);"#;
        sqlx::query(query)
            .bind(job_type)
            .bind::<u32>(self.config.buffer_size.try_into().unwrap())
            .execute(&mut *tx)
            .await?;
        Ok(())
    }

    /// Add jobs that workers have disappeared to the queue
    pub async fn reenqueue_orphaned(&self, timeout: i64) -> Result<(), sqlx::Error>
    where
        T: Job,
    {
        let job_type = T::NAME;
        let mut tx = self.pool.acquire().await?;
        let query = r#"Update Jobs
                            SET status = "Pending", done_at = NULL, lock_by = NULL, lock_at = NULL, last_error ="Job was abandoned"
                            WHERE id in
                                (SELECT Jobs.id from Jobs INNER join Workers ON lock_by = Workers.id
                                    WHERE status= "Running" AND workers.last_seen < ?1
                                    AND Workers.worker_type = ?2 ORDER BY lock_at ASC LIMIT ?3);"#;

        sqlx::query(query)
            .bind(timeout)
            .bind(job_type)
            .bind::<u32>(self.config.buffer_size.try_into().unwrap())
            .execute(&mut *tx)
            .await?;
        Ok(())
    }
}

impl<T: Job + Serialize + DeserializeOwned + Sync + Send + Unpin + 'static> Backend<Request<T>>
    for SqliteStorage<T>
{
    type Stream = BackendStream<RequestStream<Request<T>>>;
    type Layer = AckLayer<SqliteStorage<T>, T>;

    fn common_layer(&self, worker_id: WorkerId) -> Self::Layer {
        AckLayer::new(self.clone(), worker_id)
    }

    fn poll(mut self, worker: WorkerId) -> Poller<Self::Stream> {
        let config = self.config.clone();
        let controller = self.controller.clone();
        let stream = self
            .stream_jobs(&worker, config.poll_interval, config.buffer_size)
            .map_err(|e| Error::SourceError(Box::new(e)));
        let stream = BackendStream::new(stream.boxed(), controller);
        let heartbeat = async move {
            loop {
                let now: i64 = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    .try_into()
                    .unwrap();

                self.keep_alive_at::<T>(&worker, now).await.unwrap();
                apalis_core::sleep(Duration::from_secs(30)).await;
            }
        }
        .boxed();
        Poller::new(stream, heartbeat)
    }
}

impl<T: Sync> Ack<T> for SqliteStorage<T> {
    type Acknowledger = TaskId;
    type Error = sqlx::Error;
    async fn ack(
        &self,
        worker_id: &WorkerId,
        task_id: &Self::Acknowledger,
    ) -> Result<(), sqlx::Error> {
        let pool = self.pool.clone();
        let query =
                "UPDATE Jobs SET status = 'Done', done_at = strftime('%s','now') WHERE id = ?1 AND lock_by = ?2";
        sqlx::query(query)
            .bind(task_id.to_string())
            .bind(worker_id.to_string())
            .execute(&pool)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::context::State;

    use super::*;
    use email_service::Email;
    use futures::StreamExt;
    use sqlx::types::chrono::Utc;

    /// migrate DB and return a storage instance.
    async fn setup() -> SqliteStorage<Email> {
        // Because connections cannot be shared across async runtime
        // (different runtimes are created for each test),
        // we don't share the storage and tests must be run sequentially.
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        SqliteStorage::setup(&pool)
            .await
            .expect("failed to migrate DB");
        let storage = SqliteStorage::<Email>::new(pool);

        storage
    }

    #[tokio::test]
    async fn test_inmemory_sqlite_worker() {
        let mut sqlite = setup().await;
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
    }

    struct DummyService {}

    fn example_email() -> Email {
        Email {
            subject: "Test Subject".to_string(),
            to: "example@postgres".to_string(),
            text: "Some Text".to_string(),
        }
    }

    async fn consume_one(
        storage: &mut SqliteStorage<Email>,
        worker_id: &WorkerId,
    ) -> Request<Email> {
        let mut stream = storage
            .stream_jobs(worker_id, std::time::Duration::from_secs(10), 1)
            .boxed();
        stream
            .next()
            .await
            .expect("stream is empty")
            .expect("failed to poll job")
            .expect("no job is pending")
    }

    async fn register_worker_at(storage: &mut SqliteStorage<Email>, last_seen: i64) -> WorkerId {
        let worker_id = WorkerId::new("test-worker");

        storage
            .keep_alive_at::<DummyService>(&worker_id, last_seen)
            .await
            .expect("failed to register worker");
        worker_id
    }

    async fn register_worker(storage: &mut SqliteStorage<Email>) -> WorkerId {
        register_worker_at(storage, Utc::now().timestamp()).await
    }

    async fn push_email(storage: &mut SqliteStorage<Email>, email: Email) {
        storage.push(email).await.expect("failed to push a job");
    }

    async fn get_job(storage: &mut SqliteStorage<Email>, job_id: &TaskId) -> Request<Email> {
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
        let ctx = job.get::<SqlContext>().unwrap();
        assert_eq!(*ctx.status(), State::Running);
        assert_eq!(*ctx.lock_by(), Some(worker_id.clone()));
        assert!(ctx.lock_at().is_some());
    }

    #[tokio::test]
    async fn test_acknowledge_job() {
        let mut storage = setup().await;
        push_email(&mut storage, example_email()).await;

        let worker_id = register_worker(&mut storage).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let ctx = job.get::<SqlContext>().unwrap();
        let job_id = ctx.id();

        storage
            .ack(&worker_id, job_id)
            .await
            .expect("failed to acknowledge the job");

        let job = get_job(&mut storage, job_id).await;
        let ctx = job.get::<SqlContext>().unwrap();
        assert_eq!(*ctx.status(), State::Done);
        assert!(ctx.done_at().is_some());
    }

    #[tokio::test]
    async fn test_kill_job() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let worker_id = register_worker(&mut storage).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let ctx = job.get::<SqlContext>().unwrap();
        let job_id = ctx.id();

        storage
            .kill(&worker_id, job_id)
            .await
            .expect("failed to kill job");

        let job = get_job(&mut storage, job_id).await;
        let ctx = job.get::<SqlContext>().unwrap();
        assert_eq!(*ctx.status(), State::Killed);
        assert!(ctx.done_at().is_some());
    }

    #[tokio::test]
    async fn test_heartbeat_renqueueorphaned_pulse_last_seen_6min() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let six_minutes_ago = Utc::now() - Duration::from_secs(6 * 60);

        let worker_id = register_worker_at(&mut storage, six_minutes_ago.timestamp()).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let ctx = job.get::<SqlContext>().unwrap();
        storage
            .reenqueue_orphaned(six_minutes_ago.timestamp())
            .await
            .expect("failed to heartbeat");

        let job_id = ctx.id();
        let job = get_job(&mut storage, job_id).await;
        let ctx = job.get::<SqlContext>().unwrap();
        // TODO: rework these assertions
        // assert_eq!(*ctx.status(), State::Pending);
        // assert!(ctx.done_at().is_none());
        // assert!(ctx.lock_by().is_none());
        // assert!(ctx.lock_at().is_none());
        // assert_eq!(*ctx.last_error(), Some("Job was abandoned".to_string()));
    }

    #[tokio::test]
    async fn test_heartbeat_renqueueorphaned_pulse_last_seen_4min() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let four_minutes_ago = Utc::now() - Duration::from_secs(4 * 60);
        let worker_id = register_worker_at(&mut storage, four_minutes_ago.timestamp()).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let ctx = job.get::<SqlContext>().unwrap();
        storage
            .reenqueue_orphaned(four_minutes_ago.timestamp())
            .await
            .expect("failed to heartbeat");

        let job_id = ctx.id();
        let job = get_job(&mut storage, job_id).await;
        let ctx = job.get::<SqlContext>().unwrap();
        assert_eq!(*ctx.status(), State::Running);
        assert_eq!(*ctx.lock_by(), Some(worker_id));
    }
}
