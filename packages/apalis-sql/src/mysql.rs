use apalis_core::backend::{BackendExpose, Stat, WorkerState};
use apalis_core::codec::json::JsonCodec;
use apalis_core::error::{BoxDynError, Error};
use apalis_core::layers::{Ack, AckLayer};
use apalis_core::notify::Notify;
use apalis_core::poller::controller::Controller;
use apalis_core::poller::stream::BackendStream;
use apalis_core::poller::Poller;
use apalis_core::request::{Parts, Request, RequestStream, State};
use apalis_core::response::Response;
use apalis_core::storage::Storage;
use apalis_core::task::namespace::Namespace;
use apalis_core::task::task_id::TaskId;
use apalis_core::worker::{Context, Event, Worker, WorkerId};
use apalis_core::{backend::Backend, codec::Codec};
use async_stream::try_stream;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, TryStreamExt};
use log::error;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use sqlx::mysql::MySqlRow;
use sqlx::{MySql, Pool, Row};
use std::any::type_name;
use std::convert::TryInto;
use std::fmt::Debug;
use std::sync::Arc;
use std::{fmt, io};
use std::{marker::PhantomData, ops::Add, time::Duration};

use crate::context::SqlContext;
use crate::from_row::SqlRequest;
use crate::{calculate_status, Config, SqlError};

pub use sqlx::mysql::MySqlPool;

type MysqlCodec = JsonCodec<Value>;

/// Represents a [Storage] that persists to MySQL

pub struct MysqlStorage<T, C = JsonCodec<Value>>
where
    C: Codec,
{
    pool: Pool<MySql>,
    job_type: PhantomData<T>,
    controller: Controller,
    config: Config,
    codec: PhantomData<C>,
    ack_notify: Notify<(SqlContext, Response<C::Compact>)>,
}

impl<T, C> fmt::Debug for MysqlStorage<T, C>
where
    C: Codec,
    C::Compact: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MysqlStorage")
            .field("pool", &self.pool)
            .field("job_type", &"PhantomData<T>")
            .field("controller", &self.controller)
            .field("config", &self.config)
            .field("codec", &std::any::type_name::<C>())
            .field("ack_notify", &self.ack_notify)
            .finish()
    }
}

impl<T, C> Clone for MysqlStorage<T, C>
where
    C: Codec,
{
    fn clone(&self) -> Self {
        let pool = self.pool.clone();
        MysqlStorage {
            pool,
            job_type: PhantomData,
            controller: self.controller.clone(),
            config: self.config.clone(),
            codec: self.codec,
            ack_notify: self.ack_notify.clone(),
        }
    }
}

impl MysqlStorage<(), JsonCodec<Value>> {
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

impl<T> MysqlStorage<T>
where
    T: Serialize + DeserializeOwned,
{
    /// Create a new instance from a pool
    pub fn new(pool: MySqlPool) -> Self {
        Self::new_with_config(pool, Config::new(type_name::<T>()))
    }

    /// Create a new instance from a pool and custom config
    pub fn new_with_config(pool: MySqlPool, config: Config) -> Self {
        Self {
            pool,
            job_type: PhantomData,
            controller: Controller::new(),
            config,
            ack_notify: Notify::new(),
            codec: PhantomData,
        }
    }

    /// Expose the pool for other functionality, eg custom migrations
    pub fn pool(&self) -> &Pool<MySql> {
        &self.pool
    }

    /// Get the config used by the storage
    pub fn get_config(&self) -> &Config {
        &self.config
    }
}

impl<T, C> MysqlStorage<T, C>
where
    T: DeserializeOwned + Send + Sync + 'static,
    C: Codec<Compact = Value> + Send + 'static,
{
    fn stream_jobs(
        self,
        worker: &Worker<Context>,
        interval: Duration,
        buffer_size: usize,
    ) -> impl Stream<Item = Result<Option<Request<T, SqlContext>>, sqlx::Error>> {
        let pool = self.pool.clone();
        let worker = worker.clone();
        let worker_id = worker.id().to_string();

        try_stream! {
            let buffer_size = u32::try_from(buffer_size)
                    .map_err(|e| sqlx::Error::Io(io::Error::new(io::ErrorKind::InvalidInput, e)))?;
            loop {
                apalis_core::sleep(interval).await;
                if !worker.is_ready() {
                    continue;
                }
                let pool = pool.clone();
                let job_type = self.config.namespace.clone();
                let mut tx = pool.begin().await?;
                let fetch_query = "SELECT id FROM jobs
                WHERE (status='Pending' OR (status = 'Failed' AND attempts < max_attempts)) AND run_at <= NOW() AND job_type = ? ORDER BY priority DESC, run_at ASC LIMIT ? FOR UPDATE SKIP LOCKED";
                let task_ids: Vec<MySqlRow> = sqlx::query(fetch_query)
                    .bind(job_type)
                    .bind(buffer_size)
                    .fetch_all(&mut *tx).await?;
                if task_ids.is_empty() {
                    tx.rollback().await?;
                    yield None
                } else {
                    let task_ids: Vec<String> = task_ids.iter().map(|r| r.get_unchecked("id")).collect();
                    let id_params = format!("?{}", ", ?".repeat(task_ids.len() - 1));
                    let query = format!("UPDATE jobs SET status = 'Running', lock_by = ?, lock_at = NOW() WHERE id IN({}) AND status = 'Pending' AND lock_by IS NULL;", id_params);
                    let mut query = sqlx::query(&query).bind(worker_id.clone());
                    for i in &task_ids {
                        query = query.bind(i);
                    }
                    query.execute(&mut *tx).await?;
                    tx.commit().await?;

                    let fetch_query = format!("SELECT * FROM jobs WHERE ID IN ({}) ORDER BY priority DESC, run_at ASC", id_params);
                    let mut query = sqlx::query_as(&fetch_query);
                    for i in task_ids {
                        query = query.bind(i);
                    }
                    let jobs: Vec<SqlRequest<Value>> = query.fetch_all(&pool).await?;

                    for job in jobs {
                        yield {
                            let (req, ctx) = job.req.take_parts();
                            let req = C::decode(req)
                                .map_err(|e| sqlx::Error::Io(io::Error::new(io::ErrorKind::InvalidData, e)))?;
                            let mut req: Request<T, SqlContext> = Request::new_with_parts(req, ctx);
                            req.parts.namespace = Some(Namespace(self.config.namespace.clone()));
                            Some(req)
                        }
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
        let worker_type = self.config.namespace.clone();
        let storage_name = std::any::type_name::<Self>();
        let query =
            "INSERT INTO workers (id, worker_type, storage_name, layers, last_seen) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE id = ?;";
        sqlx::query(query)
            .bind(worker_id.to_string())
            .bind(worker_type)
            .bind(storage_name)
            .bind(std::any::type_name::<Service>())
            .bind(last_seen)
            .bind(worker_id.to_string())
            .execute(&mut *tx)
            .await?;
        Ok(())
    }
}

impl<T, C> Storage for MysqlStorage<T, C>
where
    T: Serialize + DeserializeOwned + Send + 'static + Unpin + Sync,
    C: Codec<Compact = Value> + Send + Sync + 'static,
    C::Error: std::error::Error + 'static + Send + Sync,
{
    type Job = T;

    type Error = sqlx::Error;

    type Context = SqlContext;

    type Compact = Value;

    async fn push_request(
        &mut self,
        job: Request<Self::Job, SqlContext>,
    ) -> Result<Parts<SqlContext>, sqlx::Error> {
        let (args, parts) = job.take_parts();
        let query =
            "INSERT INTO jobs VALUES (?, ?, ?, 'Pending', 0, ?, now(), NULL, NULL, NULL, NULL, ?)";
        let pool = self.pool.clone();

        let job = C::encode(args)
            .map_err(|e| sqlx::Error::Io(io::Error::new(io::ErrorKind::InvalidData, e)))?;
        let job_type = self.config.namespace.clone();
        sqlx::query(query)
            .bind(job)
            .bind(parts.task_id.to_string())
            .bind(job_type.to_string())
            .bind(parts.context.max_attempts())
            .bind(parts.context.priority())
            .execute(&pool)
            .await?;
        Ok(parts)
    }

    async fn push_raw_request(
        &mut self,
        job: Request<Self::Compact, SqlContext>,
    ) -> Result<Parts<SqlContext>, sqlx::Error> {
        let (args, parts) = job.take_parts();
        let query =
            "INSERT INTO jobs VALUES (?, ?, ?, 'Pending', 0, ?, now(), NULL, NULL, NULL, NULL, ?)";
        let pool = self.pool.clone();

        let job = C::encode(args)
            .map_err(|e| sqlx::Error::Io(io::Error::new(io::ErrorKind::InvalidData, e)))?;
        let job_type = self.config.namespace.clone();
        sqlx::query(query)
            .bind(job)
            .bind(parts.task_id.to_string())
            .bind(job_type.to_string())
            .bind(parts.context.max_attempts())
            .bind(parts.context.priority())
            .execute(&pool)
            .await?;
        Ok(parts)
    }

    async fn schedule_request(
        &mut self,
        req: Request<Self::Job, SqlContext>,
        on: i64,
    ) -> Result<Parts<Self::Context>, sqlx::Error> {
        let query =
            "INSERT INTO jobs VALUES (?, ?, ?, 'Pending', 0, ?, ?, NULL, NULL, NULL, NULL, ?)";
        let pool = self.pool.clone();

        let args = C::encode(&req.args)
            .map_err(|e| sqlx::Error::Io(io::Error::new(io::ErrorKind::InvalidData, e)))?;

        let on = DateTime::from_timestamp(on, 0);

        let job_type = self.config.namespace.clone();
        sqlx::query(query)
            .bind(args)
            .bind(req.parts.task_id.to_string())
            .bind(job_type)
            .bind(req.parts.context.max_attempts())
            .bind(on)
            .bind(req.parts.context.priority())
            .execute(&pool)
            .await?;
        Ok(req.parts)
    }

    async fn fetch_by_id(
        &mut self,
        job_id: &TaskId,
    ) -> Result<Option<Request<Self::Job, SqlContext>>, sqlx::Error> {
        let pool = self.pool.clone();

        let fetch_query = "SELECT * FROM jobs WHERE id = ?";
        let res: Option<SqlRequest<serde_json::Value>> = sqlx::query_as(fetch_query)
            .bind(job_id.to_string())
            .fetch_optional(&pool)
            .await?;
        match res {
            None => Ok(None),
            Some(job) => Ok(Some({
                let (req, parts) = job.req.take_parts();
                let req = C::decode(req)
                    .map_err(|e| sqlx::Error::Io(io::Error::new(io::ErrorKind::InvalidData, e)))?;
                let mut req = Request::new_with_parts(req, parts);
                req.parts.namespace = Some(Namespace(self.config.namespace.clone()));
                req
            })),
        }
    }

    async fn len(&mut self) -> Result<i64, sqlx::Error> {
        let pool = self.pool.clone();

        let query = "Select Count(*) as count from jobs where status='Pending'";
        let record = sqlx::query(query).fetch_one(&pool).await?;
        record.try_get("count")
    }

    async fn reschedule(
        &mut self,
        job: Request<T, SqlContext>,
        wait: Duration,
    ) -> Result<(), sqlx::Error> {
        let pool = self.pool.clone();
        let job_id = job.parts.task_id.clone();

        let wait: i64 = wait
            .as_secs()
            .try_into()
            .map_err(|e| sqlx::Error::Io(io::Error::new(io::ErrorKind::InvalidInput, e)))?;
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

    async fn update(&mut self, job: Request<Self::Job, SqlContext>) -> Result<(), sqlx::Error> {
        let pool = self.pool.clone();
        let ctx = job.parts.context;
        let status = ctx.status().to_string();
        let attempts = job.parts.attempt;
        let done_at = *ctx.done_at();
        let lock_by = ctx.lock_by().clone();
        let lock_at = *ctx.lock_at();
        let last_error = ctx.last_error().clone();
        let priority = *ctx.priority();
        let job_id = job.parts.task_id;
        let mut tx = pool.acquire().await?;
        let query =
                "UPDATE jobs SET status = ?, attempts = ?, done_at = ?, lock_by = ?, lock_at = ?, last_error = ?, priority = ? WHERE id = ?";
        sqlx::query(query)
            .bind(status.to_owned())
            .bind::<i64>(
                attempts
                    .current()
                    .try_into()
                    .map_err(|e| sqlx::Error::Io(io::Error::new(io::ErrorKind::InvalidInput, e)))?,
            )
            .bind(done_at)
            .bind(lock_by.map(|w| w.name().to_string()))
            .bind(lock_at)
            .bind(last_error)
            .bind(priority)
            .bind(job_id.to_string())
            .execute(&mut *tx)
            .await?;
        Ok(())
    }

    async fn is_empty(&mut self) -> Result<bool, Self::Error> {
        Ok(self.len().await? == 0)
    }

    async fn vacuum(&mut self) -> Result<usize, sqlx::Error> {
        let pool = self.pool.clone();
        let query = "Delete from jobs where status='Done'";
        let record = sqlx::query(query).execute(&pool).await?;
        Ok(record.rows_affected().try_into().unwrap_or_default())
    }
}

/// Errors that can occur while polling a MySQL database.
#[derive(thiserror::Error, Debug)]
pub enum MysqlPollError {
    /// Error during task acknowledgment.
    #[error("Encountered an error during ACK: `{0}`")]
    AckError(sqlx::Error),

    /// Error during result encoding.
    #[error("Encountered an error during encoding the result: {0}")]
    CodecError(BoxDynError),

    /// Error during a keep-alive heartbeat.
    #[error("Encountered an error during KeepAlive heartbeat: `{0}`")]
    KeepAliveError(sqlx::Error),

    /// Error during re-enqueuing orphaned tasks.
    #[error("Encountered an error during ReenqueueOrphaned heartbeat: `{0}`")]
    ReenqueueOrphanedError(sqlx::Error),
}

impl<Req, C> Backend<Request<Req, SqlContext>> for MysqlStorage<Req, C>
where
    Req: Serialize + DeserializeOwned + Sync + Send + 'static,
    C: Codec<Compact = Value> + Send + 'static + Sync,
    C::Error: std::error::Error + 'static + Send + Sync,
{
    type Stream = BackendStream<RequestStream<Request<Req, SqlContext>>>;

    type Layer = AckLayer<MysqlStorage<Req, C>, Req, SqlContext, C>;

    type Codec = C;

    fn poll(self, worker: &Worker<Context>) -> Poller<Self::Stream, Self::Layer> {
        let layer = AckLayer::new(self.clone());
        let config = self.config.clone();
        let controller = self.controller.clone();
        let pool = self.pool.clone();
        let ack_notify = self.ack_notify.clone();
        let mut hb_storage = self.clone();
        let requeue_storage = self.clone();
        let stream = self
            .stream_jobs(worker, config.poll_interval, config.buffer_size)
            .map_err(|e| Error::SourceError(Arc::new(Box::new(e))));
        let stream = BackendStream::new(stream.boxed(), controller);
        let w = worker.clone();

        let ack_heartbeat = async move {
            while let Some(ids) = ack_notify
                .clone()
                .ready_chunks(config.buffer_size)
                .next()
                .await
            {
                for (ctx, res) in ids {
                    let query = "UPDATE jobs SET status = ?, done_at = now(), last_error = ?, attempts = ? WHERE id = ? AND lock_by = ?";
                    let query = sqlx::query(query);
                    let last_result =
                        C::encode(res.inner.as_ref().map_err(|e| e.to_string())).map_err(Box::new);
                    match (last_result, ctx.lock_by()) {
                        (Ok(val), Some(worker_id)) => {
                            let query = query
                                .bind(calculate_status(&ctx, &res).to_string())
                                .bind(val)
                                .bind(res.attempt.current() as i32)
                                .bind(res.task_id.to_string())
                                .bind(worker_id.to_string());
                            if let Err(e) = query.execute(&pool).await {
                                w.emit(Event::Error(Box::new(MysqlPollError::AckError(e))));
                            }
                        }
                        (Err(error), Some(_)) => {
                            w.emit(Event::Error(Box::new(MysqlPollError::CodecError(error))));
                        }
                        _ => {
                            unreachable!(
                                "Attempted to ACK without a worker attached. This is a bug, File it on the repo"
                            );
                        }
                    }
                }

                apalis_core::sleep(config.poll_interval).await;
            }
        };
        let w = worker.clone();
        let heartbeat = async move {
            // Lets reenqueue any jobs that belonged to this worker in case of a death
            if let Err(e) = hb_storage
                .reenqueue_orphaned((config.buffer_size * 10) as i32, Utc::now())
                .await
            {
                w.emit(Event::Error(Box::new(
                    MysqlPollError::ReenqueueOrphanedError(e),
                )));
            }

            loop {
                let now = Utc::now();
                if let Err(e) = hb_storage.keep_alive_at::<Self::Layer>(w.id(), now).await {
                    w.emit(Event::Error(Box::new(MysqlPollError::KeepAliveError(e))));
                }
                apalis_core::sleep(config.keep_alive).await;
            }
        };
        let w = worker.clone();
        let reenqueue_beat = async move {
            loop {
                let dead_since = Utc::now()
                    - chrono::Duration::from_std(config.reenqueue_orphaned_after)
                        .expect("Could not calculate dead since");
                if let Err(e) = requeue_storage
                    .reenqueue_orphaned(
                        config
                            .buffer_size
                            .try_into()
                            .expect("Could not convert usize to i32"),
                        dead_since,
                    )
                    .await
                {
                    w.emit(Event::Error(Box::new(
                        MysqlPollError::ReenqueueOrphanedError(e),
                    )));
                }
                apalis_core::sleep(config.poll_interval).await;
            }
        };
        Poller::new_with_layer(
            stream,
            async {
                futures::join!(heartbeat, ack_heartbeat, reenqueue_beat);
            },
            layer,
        )
    }
}

impl<T, Res, C> Ack<T, Res, C> for MysqlStorage<T, C>
where
    T: Sync + Send,
    Res: Serialize + Send + 'static + Sync,
    C: Codec<Compact = Value> + Send,
    C::Error: Debug,
{
    type Context = SqlContext;
    type AckError = sqlx::Error;
    async fn ack(&mut self, ctx: &Self::Context, res: &Response<Res>) -> Result<(), sqlx::Error> {
        self.ack_notify
            .notify((
                ctx.clone(),
                res.map(|res| C::encode(res).expect("Could not encode result")),
            ))
            .map_err(|e| sqlx::Error::Io(io::Error::new(io::ErrorKind::BrokenPipe, e)))?;

        Ok(())
    }
}

impl<T, C: Codec> MysqlStorage<T, C> {
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
    pub async fn reenqueue_orphaned(
        &self,
        count: i32,
        dead_since: DateTime<Utc>,
    ) -> Result<bool, sqlx::Error> {
        let job_type = self.config.namespace.clone();
        let mut tx = self.pool.acquire().await?;
        let query = r#"Update jobs
                        INNER JOIN ( SELECT workers.id as worker_id, jobs.id as job_id from workers INNER JOIN jobs ON jobs.lock_by = workers.id WHERE jobs.attempts < jobs.max_attempts AND jobs.status = "Running" AND workers.last_seen < ? AND workers.worker_type = ?
                            ORDER BY lock_at ASC LIMIT ?) as workers ON jobs.lock_by = workers.worker_id AND jobs.id = workers.job_id
                        SET status = "Pending", done_at = NULL, lock_by = NULL, lock_at = NULL, last_error ="Job was abandoned", attempts = attempts + 1;"#;

        sqlx::query(query)
            .bind(dead_since)
            .bind(job_type)
            .bind(count)
            .execute(&mut *tx)
            .await?;
        Ok(true)
    }
}

impl<J: 'static + Serialize + DeserializeOwned + Unpin + Send + Sync> BackendExpose<J>
    for MysqlStorage<J>
{
    type Request = Request<J, Parts<SqlContext>>;
    type Error = SqlError;
    async fn stats(&self) -> Result<Stat, Self::Error> {
        let fetch_query = "SELECT
            COUNT(CASE WHEN status = 'Pending' THEN 1 END) AS pending,
            COUNT(CASE WHEN status = 'Running' THEN 1 END) AS running,
            COUNT(CASE WHEN status = 'Done' THEN 1 END) AS done,
            COUNT(CASE WHEN status = 'Retry' THEN 1 END) AS retry,
            COUNT(CASE WHEN status = 'Failed' THEN 1 END) AS failed,
            COUNT(CASE WHEN status = 'Killed' THEN 1 END) AS killed
        FROM jobs WHERE job_type = ?";

        let res: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(fetch_query)
            .bind(self.get_config().namespace())
            .fetch_one(self.pool())
            .await?;

        Ok(Stat {
            pending: res.0.try_into()?,
            running: res.1.try_into()?,
            dead: res.4.try_into()?,
            failed: res.3.try_into()?,
            success: res.2.try_into()?,
        })
    }

    async fn list_jobs(
        &self,
        status: &State,
        page: i32,
    ) -> Result<Vec<Self::Request>, Self::Error> {
        let status = status.to_string();
        let fetch_query = "SELECT * FROM jobs WHERE status = ? AND job_type = ? ORDER BY done_at DESC, run_at DESC LIMIT 10 OFFSET ?";
        let res: Vec<SqlRequest<serde_json::Value>> = sqlx::query_as(fetch_query)
            .bind(status)
            .bind(self.get_config().namespace())
            .bind(((page - 1) * 10).to_string())
            .fetch_all(self.pool())
            .await?;
        Ok(res
            .into_iter()
            .map(|j| {
                let (req, ctx) = j.req.take_parts();
                let req: J = MysqlCodec::decode(req).unwrap();
                Request::new_with_ctx(req, ctx)
            })
            .collect())
    }

    async fn list_workers(&self) -> Result<Vec<Worker<WorkerState>>, Self::Error> {
        let fetch_query =
            "SELECT id, layers, last_seen FROM workers WHERE worker_type = ? ORDER BY last_seen DESC LIMIT 20 OFFSET ?";
        let res: Vec<(String, String, i64)> = sqlx::query_as(fetch_query)
            .bind(self.get_config().namespace())
            .bind(0)
            .fetch_all(self.pool())
            .await?;
        Ok(res
            .into_iter()
            .map(|w| Worker::new(WorkerId::new(w.0), WorkerState::new::<Self>(w.1)))
            .collect())
    }
}

#[cfg(test)]
mod tests {

    use crate::sql_storage_tests;

    use super::*;

    use apalis_core::test_utils::DummyService;
    use email_service::Email;
    use futures::StreamExt;

    use apalis_core::generic_storage_test;
    use apalis_core::test_utils::apalis_test_service_fn;
    use apalis_core::test_utils::TestWrapper;

    generic_storage_test!(setup);

    sql_storage_tests!(setup::<Email>, MysqlStorage<Email>, Email);

    /// migrate DB and return a storage instance.
    async fn setup<T: Serialize + DeserializeOwned>() -> MysqlStorage<T> {
        let db_url = &std::env::var("DATABASE_URL").expect("No DATABASE_URL is specified");
        // Because connections cannot be shared across async runtime
        // (different runtimes are created for each test),
        // we don't share the storage and tests must be run sequentially.
        let pool = MySqlPool::connect(db_url).await.unwrap();
        MysqlStorage::setup(&pool)
            .await
            .expect("failed to migrate DB");
        let mut storage = MysqlStorage::new(pool);
        cleanup(&mut storage, &WorkerId::new("test-worker")).await;
        storage
    }

    /// rollback DB changes made by tests.
    /// Delete the following rows:
    ///  - jobs whose state is `Pending` or locked by `worker_id`
    ///  - worker identified by `worker_id`
    ///
    /// You should execute this function in the end of a test
    async fn cleanup<T>(storage: &mut MysqlStorage<T>, worker_id: &WorkerId) {
        sqlx::query("DELETE FROM jobs WHERE job_type = ?")
            .bind(storage.config.namespace())
            .execute(&storage.pool)
            .await
            .expect("failed to delete jobs");
        sqlx::query("DELETE FROM workers WHERE id = ?")
            .bind(worker_id.to_string())
            .execute(&storage.pool)
            .await
            .expect("failed to delete worker");
    }

    async fn consume_one(
        storage: &mut MysqlStorage<Email>,
        worker: &Worker<Context>,
    ) -> Request<Email, SqlContext> {
        let mut stream = storage
            .clone()
            .stream_jobs(worker, std::time::Duration::from_secs(10), 1);
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

    async fn register_worker_at(
        storage: &mut MysqlStorage<Email>,
        last_seen: DateTime<Utc>,
    ) -> Worker<Context> {
        let worker_id = WorkerId::new("test-worker");
        let wrk = Worker::new(worker_id, Context::default());
        wrk.start();
        storage
            .keep_alive_at::<DummyService>(&wrk.id(), last_seen)
            .await
            .expect("failed to register worker");
        wrk
    }

    async fn register_worker(storage: &mut MysqlStorage<Email>) -> Worker<Context> {
        let now = Utc::now();

        register_worker_at(storage, now).await
    }

    async fn push_email(storage: &mut MysqlStorage<Email>, email: Email) {
        storage.push(email).await.expect("failed to push a job");
    }

    async fn get_job(
        storage: &mut MysqlStorage<Email>,
        job_id: &TaskId,
    ) -> Request<Email, SqlContext> {
        // add a slight delay to allow background actions like ack to complete
        apalis_core::sleep(Duration::from_secs(1)).await;
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

        let worker = register_worker(&mut storage).await;

        let job = consume_one(&mut storage, &worker).await;
        let ctx = job.parts.context;
        // TODO: Fix assertions
        assert_eq!(*ctx.status(), State::Running);
        assert_eq!(*ctx.lock_by(), Some(worker.id().clone()));
        assert!(ctx.lock_at().is_some());
    }

    #[tokio::test]
    async fn test_kill_job() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let worker = register_worker(&mut storage).await;

        let job = consume_one(&mut storage, &worker).await;

        let job_id = &job.parts.task_id;

        storage
            .kill(worker.id(), job_id)
            .await
            .expect("failed to kill job");

        let job = get_job(&mut storage, job_id).await;
        let ctx = job.parts.context;
        // TODO: Fix assertions
        assert_eq!(*ctx.status(), State::Killed);
        assert!(ctx.done_at().is_some());
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
        let worker_id = WorkerId::new("test-worker");
        let worker = Worker::new(worker_id, Context::default());
        worker.start();
        let five_minutes_ago = Utc::now() - Duration::from_secs(5 * 60);

        let six_minutes_ago = Utc::now() - Duration::from_secs(60 * 6);

        storage
            .keep_alive_at::<Email>(worker.id(), six_minutes_ago)
            .await
            .unwrap();

        // fetch job
        let job = consume_one(&mut storage, &worker).await;
        let ctx = job.parts.context;

        assert_eq!(*ctx.status(), State::Running);

        storage
            .reenqueue_orphaned(1, five_minutes_ago)
            .await
            .unwrap();

        // then, the job status has changed to Pending
        let job = storage
            .fetch_by_id(&job.parts.task_id)
            .await
            .unwrap()
            .unwrap();
        let ctx = job.parts.context;
        assert_eq!(*ctx.status(), State::Pending);
        assert!(ctx.done_at().is_none());
        assert!(ctx.lock_by().is_none());
        assert!(ctx.lock_at().is_none());
        assert_eq!(*ctx.last_error(), Some("Job was abandoned".to_owned()));
        assert_eq!(job.parts.attempt.current(), 1);
    }

    #[tokio::test]
    async fn test_storage_heartbeat_reenqueuorphaned_pulse_last_seen_4min() {
        let mut storage = setup().await;

        let service = apalis_test_service_fn(|_: Request<Email, _>| async move {
            apalis_core::sleep(Duration::from_millis(500)).await;
            Ok::<_, io::Error>("success")
        });
        let (mut t, poller) = TestWrapper::new_with_service(storage.clone(), service);
        let four_minutes_ago = Utc::now() - Duration::from_secs(4 * 60);
        storage
            .keep_alive_at::<Email>(&t.worker.id(), four_minutes_ago)
            .await
            .unwrap();

        tokio::spawn(poller);

        // push an Email job
        let parts = storage
            .push(example_email())
            .await
            .expect("failed to push job");

        // register a worker responding at 4 minutes ago
        let six_minutes_ago = Utc::now() - Duration::from_secs(6 * 60);
        // heartbeat with ReenqueueOrpharned pulse
        storage
            .reenqueue_orphaned(1, six_minutes_ago)
            .await
            .unwrap();

        // then, the job status is not changed
        let job = storage.fetch_by_id(&parts.task_id).await.unwrap().unwrap();
        let ctx = job.parts.context;
        assert_eq!(*ctx.status(), State::Pending);
        assert_eq!(*ctx.lock_by(), None);
        assert!(ctx.lock_at().is_none());
        assert_eq!(*ctx.last_error(), None);
        assert_eq!(job.parts.attempt.current(), 0);

        let res = t.execute_next().await.unwrap();

        apalis_core::sleep(Duration::from_millis(1000)).await;

        let job = storage.fetch_by_id(&res.0).await.unwrap().unwrap();
        let ctx = job.parts.context;
        assert_eq!(*ctx.status(), State::Done);
        assert_eq!(*ctx.lock_by(), Some(t.worker.id().clone()));
        assert!(ctx.lock_at().is_some());
        assert_eq!(*ctx.last_error(), Some("{\"Ok\":\"success\"}".to_owned()));
        assert_eq!(job.parts.attempt.current(), 1);
    }
}
