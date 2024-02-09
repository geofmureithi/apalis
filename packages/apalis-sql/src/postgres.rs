//! # apalis-postgres
//!
//! Allows using postgres as a Backend
//!
//! ## Postgres Example
//!  ```rust,no_run
//! use apalis::prelude::*;
//! # use apalis_sql::postgres::PostgresStorage;
//! # use apalis_sql::postgres::PgPool;

//!  use email_service::Email;
//!
//!  #[tokio::main]
//!  async fn main() -> std::io::Result<()> {
//!      std::env::set_var("RUST_LOG", "debug,sqlx::query=error");
//!      let database_url = std::env::var("DATABASE_URL").expect("Must specify url to db");
//!      let pool = PgPool::connect(&database_url).await.unwrap();
//!      
//!      PostgresStorage::setup(&pool).await.unwrap();
//!      let pg: PostgresStorage<Email> = PostgresStorage::new(pool);
//!
//!      async fn send_email(job: Email, data: Data<usize>) -> Result<(), Error> {
//!          /// execute job
//!          Ok(())
//!      }
//!     // This can be even in another program/language
//!     // let query = "Select apalis.push_job('apalis::Email', json_build_object('subject', 'Test apalis', 'to', 'test1@example.com', 'text', 'Lorem Ipsum'));";
//!     // pg.execute(query).await.unwrap();
//!
//!      Monitor::<TokioExecutor>::new()
//!          .register_with_count(4, {
//!              WorkerBuilder::new(&format!("tasty-avocado"))
//!                  .data(0usize)
//!                  .source(pg)
//!                  .build_fn(send_email)
//!          })
//!          .run()
//!          .await
//!  }
//! ```
use crate::context::SqlContext;
use crate::Config;
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
use futures::{FutureExt, Stream};
use futures::{StreamExt, TryStreamExt};
use serde::{de::DeserializeOwned, Serialize};
use sqlx::postgres::PgListener;
use sqlx::types::chrono::DateTime;
use sqlx::{Pool, Postgres, Row};
use std::convert::TryInto;
use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;
use std::{marker::PhantomData, ops::Add, time::Duration};

type Timestamp = i64;

pub use sqlx::postgres::PgPool;

use crate::from_row::SqlRequest;

/// Represents a [Storage] that persists to Postgres
// #[derive(Debug)]
pub struct PostgresStorage<T> {
    pool: PgPool,
    job_type: PhantomData<T>,
    codec: Arc<
        Box<
            dyn Codec<T, serde_json::Value, Error = apalis_core::error::Error>
                + Sync
                + Send
                + 'static,
        >,
    >,
    config: Config,
    controller: Controller,
    ack_notify: Notify<(WorkerId, TaskId)>,
}

impl<T> Clone for PostgresStorage<T> {
    fn clone(&self) -> Self {
        PostgresStorage {
            pool: self.pool.clone(),
            job_type: PhantomData,
            codec: self.codec.clone(),
            config: self.config.clone(),
            controller: self.controller.clone(),
            ack_notify: self.ack_notify.clone(),
        }
    }
}

impl<T> fmt::Debug for PostgresStorage<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresStorage")
            .field("pool", &self.pool)
            .field("job_type", &"PhantomData<T>")
            .field("controller", &self.controller)
            .field("config", &self.config)
            .field(
                "codec",
                &"Arc<Box<dyn Codec<T, serde_json::Value, Error = Error> + Sync + Send + 'static>>",
            )
            .field("ack_notify", &self.ack_notify)
            .finish()
    }
}

impl<T: Job + Serialize + DeserializeOwned + Sync + Send + Unpin + 'static> Backend<Request<T>>
    for PostgresStorage<T>
{
    type Stream = BackendStream<RequestStream<Request<T>>>;

    type Layer = AckLayer<PostgresStorage<T>, T>;

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
        let ack_notify = self.ack_notify.clone();
        let pool = self.pool.clone();
        let ack_heartbeat = async move {
            while let Some(ids) = ack_notify
                .clone()
                .ready_chunks(config.buffer_size)
                .next()
                .await
            {
                let worker_ids: Vec<String> = ids.iter().map(|c| c.0.to_string()).collect();
                let task_ids: Vec<String> = ids.iter().map(|c| c.1.to_string()).collect();

                let query =
            "UPDATE apalis.jobs SET status = 'Done', done_at = now() WHERE id = ANY($1::text[]) AND lock_by = ANY($2::text[])";
                sqlx::query(query)
                    .bind(task_ids)
                    .bind(worker_ids)
                    .execute(&pool)
                    .await
                    .unwrap();
                apalis_core::sleep(config.poll_interval).await;
            }
        };
        let heartbeat = async move {
            loop {
                let now: i64 = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    .try_into()
                    .unwrap();
                self.keep_alive_at::<Self::Layer>(&worker, now)
                    .await
                    .unwrap();
                apalis_core::sleep(config.keep_alive).await;
            }
        }
        .boxed();
        Poller::new(stream, async {
            futures::join!(heartbeat, ack_heartbeat);
        })
    }
}

impl PostgresStorage<()> {
    /// Get postgres migrations without running them
    #[cfg(feature = "migrate")]
    pub fn migrations() -> sqlx::migrate::Migrator {
        sqlx::migrate!("migrations/postgres")
    }

    /// Do migrations for Postgres
    #[cfg(feature = "migrate")]
    pub async fn setup(pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {
        Self::migrations().run(pool).await?;
        Ok(())
    }
}

impl<T: Serialize + DeserializeOwned> PostgresStorage<T> {
    /// New Storage from [PgPool]
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            job_type: PhantomData,
            codec: Arc::new(Box::new(JsonCodec)),
            config: Config::default(),
            controller: Controller::new(),
            ack_notify: Notify::new(),
        }
    }

    /// Expose the pool for other functionality, eg custom migrations
    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }
}

/// A listener that listens to Postgres notifications
#[derive(Debug)]
pub struct PgListen {
    listener: PgListener,
    subscriptions: Vec<(String, PgSubscription)>,
}

/// A postgres subscription
#[derive(Debug, Clone)]
pub struct PgSubscription {
    notify: Notify<()>,
}

impl PgListen {
    /// Build a new listener.
    ///
    /// Maintaining a connection can be expensive, its encouraged you only create one [PgListen] and share it with multiple [PostgresStorage]
    pub async fn new(pool: PgPool) -> Result<Self, sqlx::Error> {
        let listener = PgListener::connect_with(&pool).await?;
        Ok(Self {
            listener,
            subscriptions: Vec::new(),
        })
    }

    /// Add a new subscription
    pub fn subscribe<T: Job>(&mut self) -> PgSubscription {
        let sub = PgSubscription {
            notify: Notify::new(),
        };
        self.subscriptions.push((T::NAME.to_owned(), sub.clone()));
        sub
    }
    /// Start listening to jobs
    pub async fn listen(mut self) -> Result<(), sqlx::Error> {
        self.listener.listen("apalis::job").await?;
        let mut notification = self.listener.into_stream();
        while let Some(Ok(res)) = notification.next().await {
            let _: Vec<_> = self
                .subscriptions
                .iter()
                .filter(|s| s.0 == res.payload())
                .map(|s| s.1.notify.notify(()))
                .collect();
        }
        Ok(())
    }
}

impl<T: DeserializeOwned + Send + Unpin + Job + 'static> PostgresStorage<T> {
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
                //  Ideally wait for a job or a tick
                apalis_core::sleep(interval).await;
                let tx = pool.clone();
                let job_type = T::NAME;
                let fetch_query = "Select * from apalis.get_jobs($1, $2, $3);";
                let jobs: Vec<SqlRequest<serde_json::Value>> = sqlx::query_as(fetch_query)
                    .bind(worker_id.to_string())
                    .bind(job_type)
                    // https://docs.rs/sqlx/latest/sqlx/postgres/types/index.html
                    .bind(i32::try_from(buffer_size).unwrap())
                    .fetch_all(&tx)
                    .await?;
                for job in jobs {

                    yield Some(Into::into(SqlRequest {
                        context: job.context,
                        req: codec.decode(&job.req).unwrap(),
                    }))
                }

            }
        }
        .boxed()
    }

    async fn keep_alive_at<Service>(
        &mut self,
        worker_id: &WorkerId,
        last_seen: Timestamp,
    ) -> Result<(), sqlx::Error> {
        let pool = self.pool.clone();
        let last_seen = DateTime::from_timestamp(last_seen, 0).unwrap();
        let worker_type = T::NAME;
        let storage_name = std::any::type_name::<Self>();
        let query = "INSERT INTO apalis.workers (id, worker_type, storage_name, layers, last_seen)
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
}

impl<T> Storage for PostgresStorage<T>
where
    T: Job + Serialize + DeserializeOwned + Send + 'static + Unpin + Sync,
{
    type Job = T;

    type Error = sqlx::Error;

    type Identifier = TaskId;

    /// Push a job to Postgres [Storage]
    ///
    /// # SQL Example
    ///
    /// ```sql
    /// Select apalis.push_job(job_type::text, job::json);
    /// ```
    async fn push(&mut self, job: Self::Job) -> Result<TaskId, sqlx::Error> {
        let id = TaskId::new();
        let query = "INSERT INTO apalis.jobs VALUES ($1, $2, $3, 'Pending', 0, 25, NOW() , NULL, NULL, NULL, NULL)";
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

    async fn schedule(&mut self, job: Self::Job, on: Timestamp) -> Result<TaskId, sqlx::Error> {
        let query =
            "INSERT INTO apalis.jobs VALUES ($1, $2, $3, 'Pending', 0, 25, $4, NULL, NULL, NULL, NULL)";
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

        let fetch_query = "SELECT * FROM apalis.jobs WHERE id = $1";
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
        let query = "Select Count(*) as count from apalis.jobs where status='Pending'";
        let record = sqlx::query(query).fetch_one(&pool).await?;
        record.try_get("count")
    }

    async fn reschedule(&mut self, job: Request<T>, wait: Duration) -> Result<(), sqlx::Error> {
        let pool = self.pool.clone();
        let ctx = job.get::<SqlContext>().unwrap();
        let job_id = ctx.id();

        let now: i64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .try_into()
            .unwrap();
        let wait: i64 = wait.as_secs().try_into().unwrap();

        let mut tx = pool.acquire().await?;
        let query =
                "UPDATE apalis.jobs SET status = 'Pending', done_at = NULL, lock_by = NULL, lock_at = NULL, run_at = $2 WHERE id = $1";

        sqlx::query(query)
            .bind(job_id.to_string())
            .bind(now.add(wait))
            .execute(&mut *tx)
            .await?;
        Ok(())
    }

    async fn update(&self, job: Request<Self::Job>) -> Result<(), sqlx::Error> {
        let pool = self.pool.clone();
        let ctx = job.get::<SqlContext>().unwrap();
        let job_id = ctx.id();
        let status = ctx.status().to_string();
        let attempts: i32 = ctx.attempts().current().try_into().unwrap();
        let done_at = *ctx.done_at();
        let lock_by = ctx.lock_by().clone();
        let lock_at = *ctx.lock_at();
        let last_error = ctx.last_error().clone();

        let mut tx = pool.acquire().await?;
        let query =
                "UPDATE apalis.jobs SET status = $1, attempts = $2, done_at = $3, lock_by = $4, lock_at = $5, last_error = $6 WHERE id = $7";
        sqlx::query(query)
            .bind(status.to_owned())
            .bind(attempts)
            .bind(done_at)
            .bind(lock_by.map(|w| w.name().to_string()))
            .bind(lock_at)
            .bind(last_error)
            .bind(job_id.to_string())
            .execute(&mut *tx)
            .await?;
        Ok(())
    }

    async fn is_empty(&self) -> Result<bool, sqlx::Error> {
        Ok(self.len().await? == 0)
    }
}

impl<T: Sync> Ack<T> for PostgresStorage<T> {
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

impl<T> PostgresStorage<T> {
    /// Kill a job
    pub async fn kill(
        &mut self,
        worker_id: &WorkerId,
        task_id: &TaskId,
    ) -> Result<(), sqlx::Error> {
        let pool = self.pool.clone();

        let mut tx = pool.acquire().await?;
        let query =
                "UPDATE apalis.jobs SET status = 'Killed', done_at = now() WHERE id = $1 AND lock_by = $2";
        sqlx::query(query)
            .bind(task_id.to_string())
            .bind(worker_id.to_string())
            .execute(&mut *tx)
            .await?;
        Ok(())
    }

    /// Puts the job instantly back into the queue
    /// Another [Worker] may consume
    pub async fn retry(
        &mut self,
        worker_id: &WorkerId,
        task_id: &TaskId,
    ) -> Result<(), sqlx::Error> {
        let pool = self.pool.clone();

        let mut tx = pool.acquire().await?;
        let query =
                "UPDATE apalis.jobs SET status = 'Pending', done_at = NULL, lock_by = NULL WHERE id = $1 AND lock_by = $2";
        sqlx::query(query)
            .bind(task_id.to_string())
            .bind(worker_id.to_string())
            .execute(&mut *tx)
            .await?;
        Ok(())
    }

    /// Reenqueue jobs that have been abandoned by their workers
    pub async fn reenqueue_orphaned(&self, count: i32) -> Result<(), sqlx::Error>
    where
        T: Job,
    {
        let job_type = T::NAME;
        let mut tx = self.pool.acquire().await?;
        let query = "Update apalis.jobs
                            SET status = 'Pending', done_at = NULL, lock_by = NULL, lock_at = NULL, last_error ='Job was abandoned'
                            WHERE id in
                                (SELECT jobs.id from apalis.jobs INNER join apalis.workers ON lock_by = workers.id
                                    WHERE status= 'Running' AND workers.last_seen < (NOW() - INTERVAL '300 seconds')
                                    AND workers.worker_type = $1 ORDER BY lock_at ASC LIMIT $2);";
        sqlx::query(query)
            .bind(job_type)
            .bind(count)
            .execute(&mut *tx)
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
    async fn setup() -> PostgresStorage<Email> {
        let db_url = &std::env::var("DATABASE_URL").expect("No DATABASE_URL is specified");
        let pool = PgPool::connect(&db_url).await.unwrap();
        // Because connections cannot be shared across async runtime
        // (different runtimes are created for each test),
        // we don't share the storage and tests must be run sequentially.
        PostgresStorage::setup(&pool).await.unwrap();
        let storage = PostgresStorage::new(pool);
        storage
    }

    /// rollback DB changes made by tests.
    /// Delete the following rows:
    ///  - jobs whose state is `Pending` or locked by `worker_id`
    ///  - worker identified by `worker_id`
    ///
    /// You should execute this function in the end of a test
    async fn cleanup(storage: PostgresStorage<Email>, worker_id: &WorkerId) {
        let mut tx = storage
            .pool
            .acquire()
            .await
            .expect("failed to get connection");
        sqlx::query("Delete from apalis.jobs where lock_by = $1 or status = 'Pending'")
            .bind(worker_id.to_string())
            .execute(&mut *tx)
            .await
            .expect("failed to delete jobs");
        sqlx::query("Delete from apalis.workers where id = $1")
            .bind(worker_id.to_string())
            .execute(&mut *tx)
            .await
            .expect("failed to delete worker");
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
        storage: &mut PostgresStorage<Email>,
        worker_id: &WorkerId,
    ) -> Request<Email> {
        let mut stream = storage.stream_jobs(worker_id, std::time::Duration::from_secs(10), 1);
        stream
            .next()
            .await
            .expect("stream is empty")
            .expect("failed to poll job")
            .expect("no job is pending")
    }

    async fn register_worker_at(
        storage: &mut PostgresStorage<Email>,
        last_seen: Timestamp,
    ) -> WorkerId {
        let worker_id = WorkerId::new("test-worker");

        storage
            .keep_alive_at::<DummyService>(&worker_id, last_seen)
            .await
            .expect("failed to register worker");
        worker_id
    }

    async fn register_worker(storage: &mut PostgresStorage<Email>) -> WorkerId {
        register_worker_at(storage, Utc::now().timestamp()).await
    }

    async fn push_email(storage: &mut PostgresStorage<Email>, email: Email) {
        storage.push(email).await.expect("failed to push a job");
    }

    async fn get_job(storage: &mut PostgresStorage<Email>, job_id: &TaskId) -> Request<Email> {
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
        // TODO: assert!(ctx.lock_at().is_some());

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
        // TODO: Currently ack is done in the background
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
        assert_eq!(*ctx.status(), State::Killed);
        // TODO: assert!(ctx.done_at().is_some());

        cleanup(storage, &worker_id).await;
    }

    #[tokio::test]
    async fn test_heartbeat_renqueueorphaned_pulse_last_seen_6min() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;
        let six_minutes_ago = Utc::now() - Duration::from_secs(6 * 60);
        let worker_id = register_worker_at(&mut storage, six_minutes_ago.timestamp()).await;

        let job = consume_one(&mut storage, &worker_id).await;
        storage
            .reenqueue_orphaned(5)
            .await
            .expect("failed to heartbeat");
        let ctx = job.get::<SqlContext>().unwrap();
        let job_id = ctx.id();
        let job = get_job(&mut storage, job_id).await;
        let ctx = job.get::<SqlContext>().unwrap();

        assert_eq!(*ctx.status(), State::Pending);
        assert!(ctx.done_at().is_none());
        assert!(ctx.lock_by().is_none());
        assert!(ctx.lock_at().is_none());
        assert_eq!(*ctx.last_error(), Some("Job was abandoned".to_string()));

        cleanup(storage, &worker_id).await;
    }

    #[tokio::test]
    async fn test_heartbeat_renqueueorphaned_pulse_last_seen_4min() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let four_minutes_ago = Utc::now() - Duration::from_secs(4 * 60);

        let worker_id = register_worker_at(&mut storage, four_minutes_ago.timestamp()).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let ctx = job.get::<SqlContext>().unwrap();

        assert_eq!(*ctx.status(), State::Running);
        storage
            .reenqueue_orphaned(5)
            .await
            .expect("failed to heartbeat");

        let job_id = ctx.id();
        let job = get_job(&mut storage, job_id).await;
        let ctx = job.get::<SqlContext>().unwrap();

        assert_eq!(*ctx.status(), State::Running);
        assert_eq!(*ctx.lock_by(), Some(worker_id.clone()));

        cleanup(storage, &worker_id).await;
    }
}
