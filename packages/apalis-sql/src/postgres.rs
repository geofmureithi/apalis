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
//!      Monitor::new()
//!          .register({
//!              WorkerBuilder::new(&format!("tasty-avocado"))
//!                  .data(0usize)
//!                  .backend(pg)
//!                  .build_fn(send_email)
//!          })
//!          .run()
//!          .await
//!  }
//! ```
use crate::context::SqlContext;
use crate::{calculate_status, Config, SqlError};
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
use chrono::{DateTime, Utc};
use futures::channel::mpsc;
use futures::StreamExt;
use futures::{select, stream, SinkExt};
use log::error;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use sqlx::postgres::PgListener;
use sqlx::{Pool, Postgres, Row};
use std::any::type_name;
use std::convert::TryInto;
use std::fmt::Debug;
use std::sync::Arc;
use std::{fmt, io};
use std::{marker::PhantomData, time::Duration};

type Timestamp = i64;

pub use sqlx::postgres::PgPool;

use crate::from_row::SqlRequest;

/// Represents a [Storage] that persists to Postgres
// #[derive(Debug)]
pub struct PostgresStorage<T, C = JsonCodec<serde_json::Value>>
where
    C: Codec,
{
    pool: PgPool,
    job_type: PhantomData<T>,
    codec: PhantomData<C>,
    config: Config,
    controller: Controller,
    ack_notify: Notify<(SqlContext, Response<C::Compact>)>,
    subscription: Option<PgSubscription>,
}

impl<T, C: Codec> Clone for PostgresStorage<T, C> {
    fn clone(&self) -> Self {
        PostgresStorage {
            pool: self.pool.clone(),
            job_type: PhantomData,
            codec: PhantomData,
            config: self.config.clone(),
            controller: self.controller.clone(),
            ack_notify: self.ack_notify.clone(),
            subscription: self.subscription.clone(),
        }
    }
}

impl<T, C: Codec> fmt::Debug for PostgresStorage<T, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresStorage")
            .field("pool", &self.pool)
            .field("job_type", &"PhantomData<T>")
            .field("controller", &self.controller)
            .field("config", &self.config)
            .field("codec", &std::any::type_name::<C>())
            // .field("ack_notify", &std::any::type_name_of_val(&self.ack_notify))
            .finish()
    }
}

/// Errors that can occur while polling a PostgreSQL database.
#[derive(thiserror::Error, Debug)]
pub enum PgPollError {
    /// Error during task acknowledgment.
    #[error("Encountered an error during ACK: `{0}`")]
    AckError(sqlx::Error),

    /// Error while fetching the next item.
    #[error("Encountered an error during FetchNext: `{0}`")]
    FetchNextError(apalis_core::error::Error),

    /// Error while listening to PostgreSQL notifications.
    #[error("Encountered an error during listening to PgNotification: {0}")]
    PgNotificationError(apalis_core::error::Error),

    /// Error during a keep-alive heartbeat.
    #[error("Encountered an error during KeepAlive heartbeat: `{0}`")]
    KeepAliveError(sqlx::Error),

    /// Error during re-enqueuing orphaned tasks.
    #[error("Encountered an error during ReenqueueOrphaned heartbeat: `{0}`")]
    ReenqueueOrphanedError(sqlx::Error),

    /// Error during result encoding.
    #[error("Encountered an error during encoding the result: {0}")]
    CodecError(BoxDynError),
}

impl<T, C> Backend<Request<T, SqlContext>> for PostgresStorage<T, C>
where
    T: Serialize + DeserializeOwned + Sync + Send + Unpin + 'static,
    C: Codec<Compact = Value> + Send + 'static,
    C::Error: std::error::Error + 'static + Send + Sync,
{
    type Stream = BackendStream<RequestStream<Request<T, SqlContext>>>;

    type Layer = AckLayer<PostgresStorage<T, C>, T, SqlContext, C>;

    type Codec = C;

    fn poll(mut self, worker: &Worker<Context>) -> Poller<Self::Stream, Self::Layer> {
        let layer = AckLayer::new(self.clone());
        let subscription = self.subscription.clone();
        let config = self.config.clone();
        let controller = self.controller.clone();
        let (mut tx, rx) = mpsc::channel(self.config.buffer_size);
        let ack_notify = self.ack_notify.clone();
        let pool = self.pool.clone();
        let worker = worker.clone();
        let heartbeat = async move {
            // Lets reenqueue any jobs that belonged to this worker in case of a death
            if let Err(e) = self
                .reenqueue_orphaned((config.buffer_size * 10) as i32, Utc::now())
                .await
            {
                worker.emit(Event::Error(Box::new(PgPollError::ReenqueueOrphanedError(
                    e,
                ))));
            }

            let mut keep_alive_stm = apalis_core::interval::interval(config.keep_alive).fuse();
            let mut reenqueue_orphaned_stm =
                apalis_core::interval::interval(config.poll_interval).fuse();

            let mut ack_stream = ack_notify.clone().ready_chunks(config.buffer_size).fuse();

            let mut poll_next_stm = apalis_core::interval::interval(config.poll_interval).fuse();

            let mut pg_notification = subscription
                .map(|stm| stm.notify.boxed().fuse())
                .unwrap_or(stream::iter(vec![]).boxed().fuse());

            async fn fetch_next_batch<
                T: Unpin + DeserializeOwned + Send + 'static,
                C: Codec<Compact = Value>,
            >(
                storage: &mut PostgresStorage<T, C>,
                worker: &WorkerId,
                tx: &mut mpsc::Sender<Result<Option<Request<T, SqlContext>>, Error>>,
            ) -> Result<(), Error> {
                let res = storage
                    .fetch_next(worker)
                    .await
                    .map_err(|e| Error::SourceError(Arc::new(Box::new(e))))?;
                for job in res {
                    tx.send(Ok(Some(job)))
                        .await
                        .map_err(|e| Error::SourceError(Arc::new(Box::new(e))))?;
                }
                Ok(())
            }

            if let Err(e) = self
                .keep_alive_at::<Self::Layer>(worker.id(), Utc::now().timestamp())
                .await
            {
                worker.emit(Event::Error(Box::new(PgPollError::KeepAliveError(e))));
            }

            loop {
                select! {
                    _ = keep_alive_stm.next() => {
                        if let Err(e) = self.keep_alive_at::<Self::Layer>(worker.id(), Utc::now().timestamp()).await {
                            worker.emit(Event::Error(Box::new(PgPollError::KeepAliveError(e))));
                        }
                    }
                    ids = ack_stream.next() => {

                        if let Some(ids) = ids {
                            let ack_ids: Vec<(String, String, String, String, u64)> = ids.iter().map(|(ctx, res)| {
                                (res.task_id.to_string(), worker.id().to_string(), serde_json::to_string(&res.inner.as_ref().map_err(|e| e.to_string())).expect("Could not convert response to json"), calculate_status(ctx,res).to_string(), res.attempt.current() as u64)
                            }).collect();
                            let query =
                                "UPDATE apalis.jobs
                                    SET status = Q.status, 
                                        done_at = now(), 
                                        lock_by = Q.worker_id, 
                                        last_error = Q.result, 
                                        attempts = Q.attempts 
                                    FROM (
                                        SELECT (value->>0)::text as id, 
                                            (value->>1)::text as worker_id, 
                                            (value->>2)::text as result, 
                                            (value->>3)::text as status, 
                                            (value->>4)::int as attempts 
                                        FROM json_array_elements($1::json)
                                    ) Q
                                    WHERE apalis.jobs.id = Q.id;
                                    ";
                            let codec_res = C::encode(&ack_ids);
                            match codec_res {
                                Ok(val) => {
                                    if let Err(e) = sqlx::query(query)
                                        .bind(val)
                                        .execute(&pool)
                                        .await
                                    {
                                        worker.emit(Event::Error(Box::new(PgPollError::AckError(e))));
                                    }
                                }
                                Err(e) => {
                                    worker.emit(Event::Error(Box::new(PgPollError::CodecError(e.into()))));
                                }
                            }

                        }
                    }
                    _ = poll_next_stm.next() => {
                        if worker.is_ready() {
                            if let Err(e) = fetch_next_batch(&mut self, worker.id(), &mut tx).await {
                                worker.emit(Event::Error(Box::new(PgPollError::FetchNextError(e))));
                            }
                        }
                    }
                    _ = pg_notification.next() => {
                        if let Err(e) = fetch_next_batch(&mut self, worker.id(), &mut tx).await {
                            worker.emit(Event::Error(Box::new(PgPollError::PgNotificationError(e))));

                        }
                    }
                    _ = reenqueue_orphaned_stm.next() => {
                        let dead_since = Utc::now()
                            - chrono::Duration::from_std(config.reenqueue_orphaned_after).expect("could not build dead_since");
                        if let Err(e) = self.reenqueue_orphaned((config.buffer_size * 10) as i32, dead_since).await {
                            worker.emit(Event::Error(Box::new(PgPollError::ReenqueueOrphanedError(e))));
                        }
                    }


                };
            }
        };
        Poller::new_with_layer(BackendStream::new(rx.boxed(), controller), heartbeat, layer)
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

impl<T> PostgresStorage<T> {
    /// New Storage from [PgPool]
    pub fn new(pool: PgPool) -> Self {
        Self::new_with_config(pool, Config::new(type_name::<T>()))
    }
    /// New Storage from [PgPool] and custom config
    pub fn new_with_config(pool: PgPool, config: Config) -> Self {
        Self {
            pool,
            job_type: PhantomData,
            codec: PhantomData,
            config,
            controller: Controller::new(),
            ack_notify: Notify::new(),
            subscription: None,
        }
    }

    /// Expose the pool for other functionality, eg custom migrations
    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }

    /// Expose the config
    pub fn config(&self) -> &Config {
        &self.config
    }
}

impl<T, C: Codec> PostgresStorage<T, C> {
    /// Expose the codec
    pub fn codec(&self) -> &PhantomData<C> {
        &self.codec
    }

    async fn keep_alive_at<Service>(
        &mut self,
        worker_id: &WorkerId,
        last_seen: Timestamp,
    ) -> Result<(), sqlx::Error> {
        let last_seen = DateTime::from_timestamp(last_seen, 0).ok_or(sqlx::Error::Io(
            io::Error::new(io::ErrorKind::InvalidInput, "Invalid Timestamp"),
        ))?;
        let worker_type = self.config.namespace.clone();
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
            .execute(&self.pool)
            .await?;
        Ok(())
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

    /// Add a new subscription with a storage
    pub fn subscribe_with<T>(&mut self, storage: &mut PostgresStorage<T>) {
        let sub = PgSubscription {
            notify: Notify::new(),
        };
        self.subscriptions
            .push((storage.config.namespace.to_owned(), sub.clone()));
        storage.subscription = Some(sub)
    }

    /// Add a new subscription
    pub fn subscribe(&mut self, namespace: &str) -> PgSubscription {
        let sub = PgSubscription {
            notify: Notify::new(),
        };
        self.subscriptions.push((namespace.to_owned(), sub.clone()));
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

impl<T, C> PostgresStorage<T, C>
where
    T: DeserializeOwned + Send + Unpin + 'static,
    C: Codec<Compact = Value>,
{
    async fn fetch_next(
        &mut self,
        worker_id: &WorkerId,
    ) -> Result<Vec<Request<T, SqlContext>>, sqlx::Error> {
        let config = &self.config;
        let job_type = &config.namespace;
        let fetch_query = "Select * from apalis.get_jobs($1, $2, $3);";
        let jobs: Vec<SqlRequest<serde_json::Value>> = sqlx::query_as(fetch_query)
            .bind(worker_id.to_string())
            .bind(job_type)
            // https://docs.rs/sqlx/latest/sqlx/postgres/types/index.html
            .bind(
                i32::try_from(config.buffer_size)
                    .map_err(|e| sqlx::Error::Io(io::Error::new(io::ErrorKind::InvalidInput, e)))?,
            )
            .fetch_all(&self.pool)
            .await?;
        let jobs: Vec<_> = jobs
            .into_iter()
            .map(|job| {
                let (req, parts) = job.req.take_parts();
                let req = C::decode(req)
                    .map_err(|e| sqlx::Error::Io(io::Error::new(io::ErrorKind::InvalidData, e)))
                    .expect("Unable to decode");
                let mut req = Request::new_with_parts(req, parts);
                req.parts.namespace = Some(Namespace(self.config.namespace.clone()));
                req
            })
            .collect();
        Ok(jobs)
    }
}

impl<Req, C> Storage for PostgresStorage<Req, C>
where
    Req: Serialize + DeserializeOwned + Send + 'static + Unpin + Sync,
    C: Codec<Compact = Value> + Send + 'static,
    C::Error: Send + std::error::Error + Sync + 'static,
{
    type Job = Req;

    type Error = sqlx::Error;

    type Context = SqlContext;

    type Compact = Value;

    /// Push a job to Postgres [Storage]
    ///
    /// # SQL Example
    ///
    /// ```sql
    /// Select apalis.push_job(job_type::text, job::json);
    /// ```
    async fn push_request(
        &mut self,
        req: Request<Self::Job, SqlContext>,
    ) -> Result<Parts<SqlContext>, sqlx::Error> {
        let query = "INSERT INTO apalis.jobs VALUES ($1, $2, $3, 'Pending', 0, $4, NOW() , NULL, NULL, NULL, NULL, $5)";

        let args = C::encode(&req.args)
            .map_err(|e| sqlx::Error::Io(io::Error::new(io::ErrorKind::InvalidData, e)))?;
        let job_type = self.config.namespace.clone();
        sqlx::query(query)
            .bind(args)
            .bind(req.parts.task_id.to_string())
            .bind(&job_type)
            .bind(req.parts.context.max_attempts())
            .bind(req.parts.context.priority())
            .execute(&self.pool)
            .await?;
        Ok(req.parts)
    }

    async fn push_raw_request(
        &mut self,
        req: Request<Self::Compact, SqlContext>,
    ) -> Result<Parts<SqlContext>, sqlx::Error> {
        let query = "INSERT INTO apalis.jobs VALUES ($1, $2, $3, 'Pending', 0, $4, NOW() , NULL, NULL, NULL, NULL, $5)";

        let args = C::encode(&req.args)
            .map_err(|e| sqlx::Error::Io(io::Error::new(io::ErrorKind::InvalidData, e)))?;
        let job_type = self.config.namespace.clone();
        sqlx::query(query)
            .bind(args)
            .bind(req.parts.task_id.to_string())
            .bind(&job_type)
            .bind(req.parts.context.max_attempts())
            .bind(req.parts.context.priority())
            .execute(&self.pool)
            .await?;
        Ok(req.parts)
    }

    async fn schedule_request(
        &mut self,
        req: Request<Self::Job, SqlContext>,
        on: Timestamp,
    ) -> Result<Parts<Self::Context>, sqlx::Error> {
        let query =
            "INSERT INTO apalis.jobs VALUES ($1, $2, $3, 'Pending', 0, $4, $5, NULL, NULL, NULL, NULL, $6)";
        let task_id = req.parts.task_id.to_string();
        let parts = req.parts;
        let on = DateTime::from_timestamp(on, 0);
        let job = C::encode(&req.args)
            .map_err(|e| sqlx::Error::Io(io::Error::new(io::ErrorKind::InvalidInput, e)))?;
        let job_type = self.config.namespace.clone();
        sqlx::query(query)
            .bind(job)
            .bind(task_id)
            .bind(job_type)
            .bind(parts.context.max_attempts())
            .bind(on)
            .bind(parts.context.priority())
            .execute(&self.pool)
            .await?;
        Ok(parts)
    }

    async fn fetch_by_id(
        &mut self,
        job_id: &TaskId,
    ) -> Result<Option<Request<Self::Job, SqlContext>>, sqlx::Error> {
        let fetch_query = "SELECT * FROM apalis.jobs WHERE id = $1 LIMIT 1";
        let res: Option<SqlRequest<serde_json::Value>> = sqlx::query_as(fetch_query)
            .bind(job_id.to_string())
            .fetch_optional(&self.pool)
            .await?;

        match res {
            None => Ok(None),
            Some(job) => Ok(Some({
                let (req, parts) = job.req.take_parts();
                let args = C::decode(req)
                    .map_err(|e| sqlx::Error::Io(io::Error::new(io::ErrorKind::InvalidData, e)))?;

                let mut req: Request<Req, SqlContext> = Request::new_with_parts(args, parts);
                req.parts.namespace = Some(Namespace(self.config.namespace.clone()));
                req
            })),
        }
    }

    async fn len(&mut self) -> Result<i64, sqlx::Error> {
        let query = "Select Count(*) as count from apalis.jobs where status='Pending' OR (status = 'Failed' AND attempts < max_attempts)";
        let record = sqlx::query(query).fetch_one(&self.pool).await?;
        record.try_get("count")
    }

    async fn reschedule(
        &mut self,
        job: Request<Req, SqlContext>,
        wait: Duration,
    ) -> Result<(), sqlx::Error> {
        let job_id = job.parts.task_id;
        let on = Utc::now() + wait;
        let mut tx = self.pool.acquire().await?;
        let query =
                "UPDATE apalis.jobs SET status = 'Pending', done_at = NULL, lock_by = NULL, lock_at = NULL, run_at = $2 WHERE id = $1";

        sqlx::query(query)
            .bind(job_id.to_string())
            .bind(on)
            .execute(&mut *tx)
            .await?;
        Ok(())
    }

    async fn update(&mut self, job: Request<Self::Job, SqlContext>) -> Result<(), sqlx::Error> {
        let ctx = job.parts.context;
        let job_id = job.parts.task_id;
        let status = ctx.status().to_string();
        let attempts: i32 = job
            .parts
            .attempt
            .current()
            .try_into()
            .map_err(|e| sqlx::Error::Io(io::Error::new(io::ErrorKind::InvalidData, e)))?;
        let done_at = *ctx.done_at();
        let lock_by = ctx.lock_by().clone();
        let lock_at = *ctx.lock_at();
        let last_error = ctx.last_error().clone();
        let priority = *ctx.priority();

        let mut tx = self.pool.acquire().await?;
        let query =
                "UPDATE apalis.jobs SET status = $1, attempts = $2, done_at = to_timestamp($3), lock_by = $4, lock_at = to_timestamp($5), last_error = $6, priority = $7 WHERE id = $8";
        sqlx::query(query)
            .bind(status.to_owned())
            .bind(attempts)
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

    async fn is_empty(&mut self) -> Result<bool, sqlx::Error> {
        Ok(self.len().await? == 0)
    }

    async fn vacuum(&mut self) -> Result<usize, sqlx::Error> {
        let query = "Delete from apalis.jobs where status='Done'";
        let record = sqlx::query(query).execute(&self.pool).await?;
        Ok(record.rows_affected().try_into().unwrap_or_default())
    }
}

impl<T, Res, C> Ack<T, Res, C> for PostgresStorage<T, C>
where
    T: Sync + Send,
    Res: Serialize + Sync + Clone,
    C: Codec<Compact = Value> + Send,
{
    type Context = SqlContext;
    type AckError = sqlx::Error;
    async fn ack(&mut self, ctx: &Self::Context, res: &Response<Res>) -> Result<(), sqlx::Error> {
        let res = res.clone().map(|r| {
            C::encode(r)
                .map_err(|e| sqlx::Error::Io(io::Error::new(io::ErrorKind::Interrupted, e)))
                .expect("Could not encode result")
        });

        self.ack_notify
            .notify((ctx.clone(), res))
            .map_err(|e| sqlx::Error::Io(io::Error::new(io::ErrorKind::Interrupted, e)))?;

        Ok(())
    }
}

impl<T, C: Codec> PostgresStorage<T, C> {
    /// Kill a job
    pub async fn kill(
        &mut self,
        worker_id: &WorkerId,
        task_id: &TaskId,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.acquire().await?;
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
    /// Another Worker may consume
    pub async fn retry(
        &mut self,
        worker_id: &WorkerId,
        task_id: &TaskId,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.acquire().await?;
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
    pub async fn reenqueue_orphaned(
        &mut self,
        count: i32,
        dead_since: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let job_type = self.config.namespace.clone();
        let mut tx = self.pool.acquire().await?;
        let query = "UPDATE apalis.jobs
                            SET status = 'Pending', done_at = NULL, lock_by = NULL, lock_at = NULL, last_error = 'Job was abandoned'
                            WHERE id IN
                                (SELECT jobs.id FROM apalis.jobs INNER JOIN apalis.workers ON lock_by = workers.id
                                    WHERE status = 'Running' 
                                    AND workers.last_seen < ($3::timestamp)
                                    AND workers.worker_type = $1 
                                    ORDER BY lock_at ASC 
                                    LIMIT $2);";

        sqlx::query(query)
            .bind(job_type)
            .bind(count)
            .bind(dead_since)
            .execute(&mut *tx)
            .await?;
        Ok(())
    }
}

impl<J: 'static + Serialize + DeserializeOwned + Unpin + Send + Sync> BackendExpose<J>
    for PostgresStorage<J>
{
    type Request = Request<J, Parts<SqlContext>>;
    type Error = SqlError;
    async fn stats(&self) -> Result<Stat, Self::Error> {
        let fetch_query = "SELECT
                            COUNT(1) FILTER (WHERE status = 'Pending') AS pending,
                            COUNT(1) FILTER (WHERE status = 'Running') AS running,
                            COUNT(1) FILTER (WHERE status = 'Done') AS done,
                            COUNT(1) FILTER (WHERE status = 'Retry') AS retry,
                            COUNT(1) FILTER (WHERE status = 'Failed') AS failed,
                            COUNT(1) FILTER (WHERE status = 'Killed') AS killed
                        FROM apalis.jobs WHERE job_type = $1";

        let res: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(fetch_query)
            .bind(self.config().namespace())
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
        let fetch_query = "SELECT * FROM apalis.jobs WHERE status = $1 AND job_type = $2 ORDER BY done_at DESC, run_at DESC LIMIT 10 OFFSET $3";
        let res: Vec<SqlRequest<serde_json::Value>> = sqlx::query_as(fetch_query)
            .bind(status)
            .bind(self.config().namespace())
            .bind(((page - 1) * 10) as i64)
            .fetch_all(self.pool())
            .await?;
        Ok(res
            .into_iter()
            .map(|j| {
                let (req, ctx) = j.req.take_parts();
                let req = JsonCodec::<Value>::decode(req).unwrap();
                Request::new_with_ctx(req, ctx)
            })
            .collect())
    }

    async fn list_workers(&self) -> Result<Vec<Worker<WorkerState>>, Self::Error> {
        let fetch_query =
            "SELECT id, layers, cast(extract(epoch from last_seen) as bigint) FROM apalis.workers WHERE worker_type = $1 ORDER BY last_seen DESC LIMIT 20 OFFSET $2";
        let res: Vec<(String, String, i64)> = sqlx::query_as(fetch_query)
            .bind(self.config().namespace())
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
    use chrono::Utc;
    use email_service::Email;

    use apalis_core::generic_storage_test;
    use apalis_core::test_utils::apalis_test_service_fn;
    use apalis_core::test_utils::TestWrapper;

    generic_storage_test!(setup);

    sql_storage_tests!(setup::<Email>, PostgresStorage<Email>, Email);

    /// migrate DB and return a storage instance.
    async fn setup<T: Serialize + DeserializeOwned>() -> PostgresStorage<T> {
        let db_url = &std::env::var("DATABASE_URL").expect("No DATABASE_URL is specified");
        let pool = PgPool::connect(&db_url).await.unwrap();
        // Because connections cannot be shared across async runtime
        // (different runtimes are created for each test),
        // we don't share the storage and tests must be run sequentially.
        PostgresStorage::setup(&pool).await.unwrap();
        let config = Config::new("apalis-tests").set_buffer_size(1);
        let mut storage = PostgresStorage::new_with_config(pool, config);
        cleanup(&mut storage, &WorkerId::new("test-worker")).await;
        storage
    }

    /// rollback DB changes made by tests.
    /// Delete the following rows:
    ///  - jobs of the current type
    ///  - worker identified by `worker_id`
    ///
    /// You should execute this function in the end of a test
    async fn cleanup<T>(storage: &mut PostgresStorage<T>, worker_id: &WorkerId) {
        let mut tx = storage
            .pool
            .acquire()
            .await
            .expect("failed to get connection");
        sqlx::query("Delete from apalis.jobs where job_type = $1 OR lock_by = $2")
            .bind(storage.config.namespace())
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
    ) -> Request<Email, SqlContext> {
        let req = storage.fetch_next(worker_id).await;
        req.unwrap()[0].clone()
    }

    async fn register_worker_at(
        storage: &mut PostgresStorage<Email>,
        last_seen: Timestamp,
    ) -> Worker<Context> {
        let worker_id = WorkerId::new("test-worker");

        storage
            .keep_alive_at::<DummyService>(&worker_id, last_seen)
            .await
            .expect("failed to register worker");
        let wrk = Worker::new(worker_id, Context::default());
        wrk.start();
        wrk
    }

    async fn register_worker(storage: &mut PostgresStorage<Email>) -> Worker<Context> {
        register_worker_at(storage, Utc::now().timestamp()).await
    }

    async fn push_email(storage: &mut PostgresStorage<Email>, email: Email) -> TaskId {
        storage
            .push(email)
            .await
            .expect("failed to push a job")
            .task_id
    }

    async fn get_job(
        storage: &mut PostgresStorage<Email>,
        job_id: &TaskId,
    ) -> Request<Email, SqlContext> {
        // add a slight delay to allow background actions like ack to complete
        apalis_core::sleep(Duration::from_secs(2)).await;
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

        let job = consume_one(&mut storage, &worker.id()).await;
        let job_id = &job.parts.task_id;

        // Refresh our job
        let job = get_job(&mut storage, job_id).await;
        let ctx = job.parts.context;
        assert_eq!(*ctx.status(), State::Running);
        assert_eq!(*ctx.lock_by(), Some(worker.id().clone()));
        assert!(ctx.lock_at().is_some());
    }

    #[tokio::test]
    async fn test_kill_job() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let worker = register_worker(&mut storage).await;

        let job = consume_one(&mut storage, &worker.id()).await;
        let job_id = &job.parts.task_id;

        storage
            .kill(&worker.id(), job_id)
            .await
            .expect("failed to kill job");

        let job = get_job(&mut storage, job_id).await;
        let ctx = job.parts.context;
        assert_eq!(*ctx.status(), State::Killed);
        assert!(ctx.done_at().is_some());
    }

    #[tokio::test]
    async fn test_heartbeat_renqueueorphaned_pulse_last_seen_6min() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;
        let six_minutes_ago = Utc::now() - Duration::from_secs(6 * 60);
        let five_minutes_ago = Utc::now() - Duration::from_secs(5 * 60);

        let worker = register_worker_at(&mut storage, six_minutes_ago.timestamp()).await;

        let job = consume_one(&mut storage, &worker.id()).await;
        storage
            .reenqueue_orphaned(1, five_minutes_ago)
            .await
            .expect("failed to heartbeat");
        let job_id = &job.parts.task_id;
        let job = get_job(&mut storage, job_id).await;
        let ctx = job.parts.context;

        assert_eq!(*ctx.status(), State::Pending);
        assert!(ctx.done_at().is_none());
        assert!(ctx.lock_by().is_none());
        assert!(ctx.lock_at().is_none());
        assert_eq!(*ctx.last_error(), Some("Job was abandoned".to_owned()));
        assert_eq!(job.parts.attempt.current(), 0); // TODO: update get_jobs to increase attempts
    }

    #[tokio::test]
    async fn test_heartbeat_renqueueorphaned_pulse_last_seen_4min() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let four_minutes_ago = Utc::now() - Duration::from_secs(4 * 60);
        let six_minutes_ago = Utc::now() - Duration::from_secs(6 * 60);

        let worker = register_worker_at(&mut storage, four_minutes_ago.timestamp()).await;

        let job = consume_one(&mut storage, &worker.id()).await;
        let ctx = &job.parts.context;

        assert_eq!(*ctx.status(), State::Running);
        storage
            .reenqueue_orphaned(1, six_minutes_ago)
            .await
            .expect("failed to heartbeat");

        let job_id = &job.parts.task_id;
        let job = get_job(&mut storage, job_id).await;
        let ctx = job.parts.context;
        assert_eq!(*ctx.status(), State::Running);
        assert_eq!(*ctx.lock_by(), Some(worker.id().clone()));
        assert!(ctx.lock_at().is_some());
        assert_eq!(*ctx.last_error(), None);
        assert_eq!(job.parts.attempt.current(), 0);
    }

    // This test pushes a scheduled request (scheduled 5 minutes in the future)
    // and then asserts that fetch_next returns nothing.
    #[tokio::test]
    async fn test_scheduled_request_not_fetched() {
        // Setup storage using the provided helper; scheduled jobs use the same table as regular ones.
        let mut storage = setup().await;

        // Schedule a request 5 minutes in the future.
        let run_at = Utc::now().timestamp() + 300; // 5 minutes = 300 secs
        let scheduled_req = Request::new(example_email());

        storage
            .schedule_request(scheduled_req, run_at)
            .await
            .expect("failed to schedule request");

        // Fetch the next jobs for a worker; expect empty since the job is scheduled for the future.
        let worker = register_worker(&mut storage).await;
        let jobs = storage
            .fetch_next(worker.id())
            .await
            .expect("failed to fetch next jobs");
        assert!(
            jobs.is_empty(),
            "Scheduled job should not be fetched before its scheduled time"
        );

        // List jobs with status 'Pending' and expect the scheduled job to be there.
        let jobs = storage
            .list_jobs(&State::Pending, 1)
            .await
            .expect("failed to list jobs");
        assert_eq!(jobs.len(), 1, "Expected one job to be listed");
    }

    // This test pushes a request using one job_type, then uses a worker with a different job_type
    // to fetch jobs and asserts that it returns nothing.
    #[tokio::test]
    async fn test_fetch_with_different_job_type_returns_empty() {
        // Setup one storage with its config namespace (job_type)
        let mut storage_email = setup().await;

        // Create a second storage using the same pool but with a different namespace.
        let pool = storage_email.pool().clone();
        let sms_config = Config::new("sms-test").set_buffer_size(1);
        let mut storage_sms: PostgresStorage<Email> =
            PostgresStorage::new_with_config(pool, sms_config);

        // Push a job using the first storage (job_type = storage_email.config.namespace)
        push_email(&mut storage_email, example_email()).await;

        // Attempt to fetch the job with a worker associated with the different job_type.
        let worker_id = WorkerId::new("sms-worker");
        let worker = Worker::new(worker_id, Context::default());
        worker.start();

        let jobs = storage_sms
            .fetch_next(worker.id())
            .await
            .expect("failed to fetch next jobs");
        assert!(
            jobs.is_empty(),
            "A worker with a different job_type should not fetch jobs"
        );

        // Fetch the job with a worker associated with the correct job_type.
        let worker = register_worker(&mut storage_email).await;
        let jobs = storage_email
            .fetch_next(worker.id())
            .await
            .expect("failed to fetch next jobs");
        assert!(!jobs.is_empty(), "Worker should fetch the job");
    }
}
