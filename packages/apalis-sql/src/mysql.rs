use apalis_core::codec::json::JsonCodec;
use apalis_core::error::Error;
use apalis_core::layers::{Ack, AckLayer};
use apalis_core::notify::Notify;
use apalis_core::poller::controller::Controller;
use apalis_core::poller::stream::BackendStream;
use apalis_core::poller::Poller;
use apalis_core::request::{Request, RequestStream};
use apalis_core::storage::{Job, Storage};
use apalis_core::task::task_id::TaskId;
use apalis_core::worker::WorkerId;
use apalis_core::{Backend, Codec};
use async_stream::try_stream;
use futures::{Stream, StreamExt, TryStreamExt};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use sqlx::mysql::MySqlRow;
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::{MySql, Pool, Row};
use std::convert::TryInto;
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;
use std::{marker::PhantomData, ops::Add, time::Duration};

use crate::context::SqlContext;
use crate::from_row::SqlRequest;
use crate::Config;

pub use sqlx::mysql::MySqlPool;

/// Represents a [Storage] that persists to MySQL

pub struct MysqlStorage<T> {
    pool: Pool<MySql>,
    job_type: PhantomData<T>,
    controller: Controller,
    config: Config,
    codec: Arc<Box<dyn Codec<T, serde_json::Value, Error = Error> + Sync + Send + 'static>>,
    ack_notify: Notify<(WorkerId, TaskId)>,
}

impl<T> fmt::Debug for MysqlStorage<T> {
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
            .field("ack_notify", &self.ack_notify)
            .finish()
    }
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
            ack_notify: self.ack_notify.clone(),
        }
    }
}

impl MysqlStorage<()> {
    /// Get mysql migrations without running them
    #[cfg(feature = "migrate")]
    pub fn migrations() -> sqlx::migrate::Migrator {
        sqlx::migrate!("migrations/mysql")
    }

    /// Do migrations for mysql
    #[cfg(feature = "migrate")]
    pub async fn setup(pool: &Pool<MySql>) -> Result<(), sqlx::Error> {
        Self::migrations().run(pool).await?;
        Ok(())
    }
}

impl<T: Serialize + DeserializeOwned> MysqlStorage<T> {
    /// Create a new instance from a pool
    pub fn new(pool: MySqlPool) -> Self {
        Self {
            pool,
            job_type: PhantomData,
            controller: Controller::new(),
            config: Config::default(),
            codec: Arc::new(Box::new(JsonCodec)),
            ack_notify: Notify::new(),
        }
    }

    /// Expose the pool for other functionality, eg custom migrations
    pub fn pool(&self) -> &Pool<MySql> {
        &self.pool
    }
}

impl<T: DeserializeOwned + Send + Unpin + Job + Sync + 'static> MysqlStorage<T> {
    fn stream_jobs(
        self,
        worker_id: &WorkerId,
        interval: Duration,
        buffer_size: usize,
    ) -> impl Stream<Item = Result<Option<Request<T>>, sqlx::Error>> {
        let pool = self.pool.clone();
        let worker_id = worker_id.to_string();

        try_stream! {
            let pool = pool.clone();
            loop {
                apalis_core::sleep(interval).await;
                let pool = pool.clone();
                let job_type = T::NAME;
                let mut tx = pool.begin().await?;
                let fetch_query = "SELECT id FROM jobs
                WHERE status = 'Pending' AND run_at <= NOW() AND job_type = ? ORDER BY run_at ASC LIMIT ? FOR UPDATE SKIP LOCKED";
                let task_ids: Vec<MySqlRow> = sqlx::query(fetch_query)
                    .bind(job_type)
                    .bind(u32::try_from(buffer_size).unwrap())
                    .fetch_all(&mut *tx).await.unwrap();
                if task_ids.is_empty() {
                    tx.rollback().await?;
                    yield None
                } else {
                    let task_ids: Vec<String> = task_ids.iter().map(|r| r.try_get("id").unwrap()).collect();
                    let id_params = format!("?{}", ", ?".repeat(task_ids.len() - 1));
                    let query = format!("UPDATE jobs SET status = 'Running', lock_by = ?, lock_at = NOW(), attempts = attempts + 1 WHERE id IN({}) AND status = 'Pending' AND lock_by IS NULL;", id_params);
                    let mut query = sqlx::query(&query).bind(worker_id.clone());
                    for i in &task_ids {
                        query = query.bind(i);
                    }
                    query.execute(&mut *tx).await?;
                    tx.commit().await?;

                    let fetch_query = format!("SELECT * FROM jobs WHERE ID IN ({})", id_params);
                    let mut query = sqlx::query_as(&fetch_query);
                    for i in task_ids {
                        query = query.bind(i);
                    }
                    let jobs: Vec<SqlRequest<Value>> = query.fetch_all(&pool).await.unwrap();

                    for job in jobs {
                        yield Some(Into::into(SqlRequest {
                            context: job.context,
                            req: self.codec.decode(&job.req).unwrap(),
                        }))
                    }
                }
            }
        }.boxed()
    }

    async fn keep_alive_at<Service>(
        &mut self,
        worker_id: &WorkerId,
        last_seen: DateTime<Utc>,
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

        let job = self.codec.encode(&job).unwrap();
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

        let job = self.codec.encode(&job).unwrap();

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
        let res: Option<SqlRequest<serde_json::Value>> = sqlx::query_as(fetch_query)
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

    async fn len(&self) -> Result<i64, sqlx::Error> {
        let pool = self.pool.clone();

        let query = "Select Count(*) as count from jobs where status='Pending'";
        let record = sqlx::query(query).fetch_one(&pool).await?;
        record.try_get("count")
    }

    async fn reschedule(&mut self, job: Request<T>, wait: Duration) -> Result<(), sqlx::Error> {
        let pool = self.pool.clone();
        let job_id = job.get::<TaskId>().unwrap();

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
                "UPDATE jobs SET status = ?, attempts = ?, done_at = ?, lock_by = ?, lock_at = ?, last_error = ? WHERE id = ?";
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
        Ok(self.len().await? == 0)
    }
}

impl<T: Job + Serialize + DeserializeOwned + Sync + Send + Unpin + 'static> Backend<Request<T>>
    for MysqlStorage<T>
{
    type Stream = BackendStream<RequestStream<Request<T>>>;

    type Layer = AckLayer<MysqlStorage<T>, T>;

    fn common_layer(&self, worker_id: WorkerId) -> Self::Layer {
        AckLayer::new(self.clone(), worker_id)
    }

    fn poll(self, worker: WorkerId) -> Poller<Self::Stream> {
        let config = self.config.clone();
        let controller = self.controller.clone();
        let pool = self.pool.clone();
        let ack_notify = self.ack_notify.clone();
        let mut hb_storage = self.clone();
        let stream = self
            .stream_jobs(&worker, config.poll_interval, config.buffer_size)
            .map_err(|e| Error::SourceError(Box::new(e)));
        let stream = BackendStream::new(stream.boxed(), controller);

        let ack_heartbeat = async move {
            while let Some(ids) = ack_notify
                .clone()
                .ready_chunks(config.buffer_size)
                .next()
                .await
            {
                let worker_ids: Vec<String> = ids.iter().map(|c| c.0.to_string()).collect();
                let task_ids: Vec<String> = ids.iter().map(|c| c.1.to_string()).collect();
                let id_params = format!("?{}", ", ?".repeat(task_ids.len() - 1));
                let worker_params = format!("?{}", ", ?".repeat(worker_ids.len() - 1));
                let query =
                format!("UPDATE jobs SET status = 'Done', done_at = now() WHERE id IN ( { } ) AND lock_by IN ( { } )", id_params, worker_params);
                let mut query = sqlx::query(&query);
                for i in task_ids {
                    query = query.bind(i);
                }
                for i in worker_ids {
                    query = query.bind(i);
                }
                query.execute(&pool).await.unwrap();
                apalis_core::sleep(config.poll_interval).await;
            }
        };

        let heartbeat = async move {
            loop {
                let now = Utc::now();
                hb_storage
                    .keep_alive_at::<Self::Layer>(&worker, now)
                    .await
                    .unwrap();
                apalis_core::sleep(config.keep_alive).await;
            }
        };
        Poller::new(stream, async {
            futures::join!(heartbeat, ack_heartbeat);
        })
    }
}

impl<T: Sync> Ack<T> for MysqlStorage<T> {
    type Acknowledger = TaskId;
    type Error = sqlx::Error;
    async fn ack(
        &self,
        worker_id: &WorkerId,
        task_id: &Self::Acknowledger,
    ) -> Result<(), sqlx::Error> {
        self.ack_notify
            .notify((worker_id.clone(), task_id.clone()))
            .unwrap();

        Ok(())
    }
}

impl<T: Job> MysqlStorage<T> {
    /// Kill a job
    pub async fn kill(&mut self, worker_id: &WorkerId, job_id: &TaskId) -> Result<(), sqlx::Error> {
        let pool = self.pool.clone();

        let mut tx = pool.acquire().await?;
        let query =
            "UPDATE jobs SET status = 'Killed', done_at = NOW() WHERE id = ? AND lock_by = ?";
        sqlx::query(query)
            .bind(job_id.to_string())
            .bind(worker_id.to_string())
            .execute(&mut *tx)
            .await?;
        Ok(())
    }

    /// Puts the job instantly back into the queue
    pub async fn retry(
        &mut self,
        worker_id: &WorkerId,
        job_id: &TaskId,
    ) -> Result<(), sqlx::Error> {
        let pool = self.pool.clone();

        let mut tx = pool.acquire().await?;
        let query =
                "UPDATE jobs SET status = 'Pending', done_at = NULL, lock_by = NULL WHERE id = ? AND lock_by = ?";
        sqlx::query(query)
            .bind(job_id.to_string())
            .bind(worker_id.to_string())
            .execute(&mut *tx)
            .await?;
        Ok(())
    }

    /// Readd jobs that are abandoned to the queue
    pub async fn reenqueue_orphaned(&self, timeout: i64) -> Result<bool, sqlx::Error> {
        let job_type = T::NAME;
        let mut tx = self.pool.acquire().await?;
        let query = r#"Update jobs
                        INNER JOIN ( SELECT workers.id as worker_id, jobs.id as job_id from workers INNER JOIN jobs ON jobs.lock_by = workers.id WHERE jobs.status = "Running" AND workers.last_seen < ? AND workers.worker_type = ?
                            ORDER BY lock_at ASC LIMIT ?) as workers ON jobs.lock_by = workers.worker_id AND jobs.id = workers.job_id
                        SET status = "Pending", done_at = NULL, lock_by = NULL, lock_at = NULL, last_error ="Job was abandoned";"#;
        let now: i64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .try_into()
            .unwrap();
        let seconds_ago = DateTime::from_timestamp(now - timeout, 0).unwrap();

        sqlx::query(query)
            .bind(seconds_ago)
            .bind(job_type)
            .bind::<i64>(self.config.buffer_size.try_into().unwrap())
            .execute(&mut *tx)
            .await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {

    use crate::context::State;

    use super::*;
    use email_service::Email;
    use futures::StreamExt;

    /// migrate DB and return a storage instance.
    async fn setup() -> MysqlStorage<Email> {
        let db_url = &std::env::var("DATABASE_URL").expect("No DATABASE_URL is specified");
        // Because connections cannot be shared across async runtime
        // (different runtimes are created for each test),
        // we don't share the storage and tests must be run sequentially.
        let pool = MySqlPool::connect(db_url).await.unwrap();
        MysqlStorage::setup(&pool)
            .await
            .expect("failed to migrate DB");
        let storage = MysqlStorage::new(pool);

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

    async fn consume_one(storage: &MysqlStorage<Email>, worker_id: &WorkerId) -> Request<Email> {
        let storage = storage.clone();
        let mut stream = storage.stream_jobs(worker_id, std::time::Duration::from_secs(10), 1);
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
        last_seen: DateTime<Utc>,
    ) -> WorkerId {
        let worker_id = WorkerId::new("test-worker");

        storage
            .keep_alive_at::<DummyService>(&worker_id, last_seen)
            .await
            .expect("failed to register worker");
        worker_id
    }

    async fn register_worker(storage: &mut MysqlStorage<Email>) -> WorkerId {
        let now = Utc::now();

        register_worker_at(storage, now).await
    }

    async fn push_email<S>(storage: &mut S, email: Email)
    where
        S: Storage<Job = Email, Error = sqlx::Error>,
    {
        storage.push(email).await.expect("failed to push a job");
    }

    async fn get_job<S>(storage: &mut S, job_id: &TaskId) -> Request<Email>
    where
        S: Storage<Job = Email, Identifier = TaskId, Error = sqlx::Error>,
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
        let ctx = job.get::<SqlContext>().unwrap();
        // TODO: Fix assertions
        // assert_eq!(*ctx.status(), State::Running);
        // assert_eq!(*ctx.lock_by(), Some(worker_id.clone()));
        // assert!(ctx.lock_at().is_some());

        cleanup(storage, &worker_id).await;
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

        // TODO: Fix assertions
        // assert_eq!(*ctx.status(), State::Done);
        // assert!(ctx.done_at().is_some());

        cleanup(storage, &worker_id).await;
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
        // TODO: Fix assertions
        // assert_eq!(*ctx.status(), State::Killed);
        // assert!(ctx.done_at().is_some());

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

        let six_minutes_ago = Utc::now() - Duration::from_secs(60 * 6);

        storage
            .keep_alive_at::<Email>(&worker_id, six_minutes_ago)
            .await
            .unwrap();

        // fetch job
        let job = consume_one(&mut storage, &worker_id).await;
        let ctx = job.get::<SqlContext>().unwrap();

        assert_eq!(*ctx.status(), State::Running);

        storage.reenqueue_orphaned(300).await.unwrap();

        // then, the job status has changed to Pending
        let job = storage.fetch_by_id(ctx.id()).await.unwrap().unwrap();
        let context = job.get::<SqlContext>().unwrap();
        // TODO: Fix assertions
        // assert_eq!(*context.status(), State::Pending);
        // assert!(context.lock_by().is_none());
        // assert!(context.lock_at().is_none());
        // assert!(context.done_at().is_none());
        // assert_eq!(*context.last_error(), Some("Job was abandoned".to_string()));

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
        let four_minutes_ago = Utc::now() - Duration::from_secs(4 * 60);

        let worker_id = WorkerId::new("test_worker");
        storage
            .keep_alive_at::<Email>(&worker_id, four_minutes_ago)
            .await
            .unwrap();

        // fetch job
        let job = consume_one(&mut storage, &worker_id).await;
        let ctx = job.get::<SqlContext>().unwrap();

        assert_eq!(*ctx.status(), State::Running);

        // heartbeat with ReenqueueOrpharned pulse
        storage.reenqueue_orphaned(300).await.unwrap();

        // then, the job status is not changed
        let job = storage.fetch_by_id(ctx.id()).await.unwrap().unwrap();
        let context = job.get::<SqlContext>().unwrap();
        // TODO: Fix assertions
        // assert_eq!(*context.status(), State::Running);
        // assert_eq!(*context.lock_by(), Some(worker_id.clone()));

        cleanup(storage, &worker_id).await;
    }
}
