use apalis_core::codec::json::JsonCodec;
use apalis_core::error::{BoxDynError, Error};
use apalis_core::layers::{Ack, AckLayer};
use apalis_core::poller::controller::Controller;
use apalis_core::poller::stream::BackendStream;
use apalis_core::poller::Poller;
use apalis_core::request::{Parts, Request, RequestStream};
use apalis_core::response::Response;
use apalis_core::service_fn::FromRequest;
use apalis_core::storage::Storage;
use apalis_core::task::namespace::Namespace;
use apalis_core::task::task_id::TaskId;
use apalis_core::worker::{Event, Worker, WorkerId};
use apalis_core::{backend::Backend, codec::Codec};
use chrono::{DateTime, Utc};
use futures::channel::mpsc::{self, SendError, Sender};
use futures::{select, FutureExt, SinkExt, StreamExt, TryFutureExt};
use log::*;
use redis::aio::ConnectionLike;
use redis::ErrorKind;
use redis::{aio::ConnectionManager, Client, IntoConnectionInfo, RedisError, Script, Value};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::any::type_name;
use std::fmt::{self, Debug};
use std::io;
use std::num::TryFromIntError;
use std::time::SystemTime;
use std::{marker::PhantomData, time::Duration};

/// Shorthand to create a client and connect
pub async fn connect<S: IntoConnectionInfo>(redis: S) -> Result<ConnectionManager, RedisError> {
    let client = Client::open(redis.into_connection_info()?)?;
    let conn = client.get_connection_manager().await?;
    Ok(conn)
}

const ACTIVE_JOBS_LIST: &str = "{queue}:active";
const CONSUMERS_SET: &str = "{queue}:consumers";
const DEAD_JOBS_SET: &str = "{queue}:dead";
const DONE_JOBS_SET: &str = "{queue}:done";
const FAILED_JOBS_SET: &str = "{queue}:failed";
const INFLIGHT_JOB_SET: &str = "{queue}:inflight";
const JOB_DATA_HASH: &str = "{queue}:data";
const SCHEDULED_JOBS_SET: &str = "{queue}:scheduled";
const SIGNAL_LIST: &str = "{queue}:signal";

/// Represents redis key names for various components of the RedisStorage.
///
/// This struct defines keys used in Redis to manage jobs and their lifecycle in the storage.
#[derive(Clone, Debug)]
pub struct RedisQueueInfo {
    /// Key for the list of currently active jobs.
    pub active_jobs_list: String,

    /// Key for the set of active consumers.
    pub consumers_set: String,

    /// Key for the set of jobs that are no longer retryable.
    pub dead_jobs_set: String,

    /// Key for the set of jobs that have completed successfully.
    pub done_jobs_set: String,

    /// Key for the set of jobs that have failed.
    pub failed_jobs_set: String,

    /// Key for the set of jobs that are currently being processed.
    pub inflight_jobs_set: String,

    /// Key for the hash storing data for each job.
    pub job_data_hash: String,

    /// Key for the set of jobs scheduled for future execution.
    pub scheduled_jobs_set: String,

    /// Key for the list used for signaling and communication between consumers and producers.
    pub signal_list: String,
}

#[derive(Clone, Debug)]
pub(crate) struct RedisScript {
    done_job: Script,
    enqueue_scheduled: Script,
    get_jobs: Script,
    kill_job: Script,
    push_job: Script,
    reenqueue_active: Script,
    reenqueue_orphaned: Script,
    register_consumer: Script,
    retry_job: Script,
    schedule_job: Script,
    vacuum: Script,
    pub(crate) stats: Script,
}

/// The context for a redis storage job
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RedisContext {
    max_attempts: usize,
    lock_by: Option<WorkerId>,
    run_at: Option<SystemTime>,
}

impl Default for RedisContext {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            lock_by: None,
            run_at: None,
        }
    }
}

impl<Req> FromRequest<Request<Req, RedisContext>> for RedisContext {
    fn from_request(req: &Request<Req, RedisContext>) -> Result<Self, Error> {
        Ok(req.parts.context.clone())
    }
}

/// Errors that can occur while polling a Redis backend.
#[derive(thiserror::Error, Debug)]
pub enum RedisPollError {
    /// Error during a keep-alive heartbeat.
    #[error("KeepAlive heartbeat encountered an error: `{0}`")]
    KeepAliveError(RedisError),

    /// Error during enqueueing scheduled tasks.
    #[error("EnqueueScheduled heartbeat encountered an error: `{0}`")]
    EnqueueScheduledError(RedisError),

    /// Error during polling for the next task or message.
    #[error("PollNext heartbeat encountered an error: `{0}`")]
    PollNextError(RedisError),

    /// Error during enqueueing tasks for worker consumption.
    #[error("Enqueue for worker consumption encountered an error: `{0}`")]
    EnqueueError(SendError),

    /// Error during acknowledgment of tasks.
    #[error("Ack heartbeat encountered an error: `{0}`")]
    AckError(RedisError),

    /// Error during re-enqueuing orphaned tasks.
    #[error("ReenqueueOrphaned heartbeat encountered an error: `{0}`")]
    ReenqueueOrphanedError(RedisError),
}

/// Config for a [RedisStorage]
#[derive(Clone, Debug)]
pub struct Config {
    poll_interval: Duration,
    buffer_size: usize,
    keep_alive: Duration,
    enqueue_scheduled: Duration,
    reenqueue_orphaned_after: Duration,
    namespace: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(100),
            buffer_size: 10,
            keep_alive: Duration::from_secs(30),
            enqueue_scheduled: Duration::from_secs(30),
            reenqueue_orphaned_after: Duration::from_secs(300),
            namespace: String::from("apalis_redis"),
        }
    }
}

impl Config {
    /// Get the interval of polling
    pub fn get_poll_interval(&self) -> &Duration {
        &self.poll_interval
    }

    /// Get the number of jobs to fetch
    pub fn get_buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// get the keep live rate
    pub fn get_keep_alive(&self) -> &Duration {
        &self.keep_alive
    }

    /// get the enqueued setting
    pub fn get_enqueue_scheduled(&self) -> &Duration {
        &self.enqueue_scheduled
    }

    /// get the namespace
    pub fn get_namespace(&self) -> &String {
        &self.namespace
    }

    /// get the poll interval
    pub fn set_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// set the buffer setting
    pub fn set_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// set the keep-alive setting
    pub fn set_keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    /// get the enqueued setting
    pub fn set_enqueue_scheduled(mut self, enqueue_scheduled: Duration) -> Self {
        self.enqueue_scheduled = enqueue_scheduled;
        self
    }

    /// set the namespace for the Storage
    pub fn set_namespace(mut self, namespace: &str) -> Self {
        self.namespace = namespace.to_string();
        self
    }

    /// Returns the Redis key for the list of pending jobs associated with the queue.
    /// The key is dynamically generated using the namespace of the queue.
    ///
    /// # Returns
    /// A `String` representing the Redis key for the pending jobs list.
    pub fn active_jobs_list(&self) -> String {
        ACTIVE_JOBS_LIST.replace("{queue}", &self.namespace)
    }

    /// Returns the Redis key for the set of consumers associated with the queue.
    /// The key is dynamically generated using the namespace of the queue.
    ///
    /// # Returns
    /// A `String` representing the Redis key for the consumers set.
    pub fn consumers_set(&self) -> String {
        CONSUMERS_SET.replace("{queue}", &self.namespace)
    }

    /// Returns the Redis key for the set of dead jobs associated with the queue.
    /// The key is dynamically generated using the namespace of the queue.
    ///
    /// # Returns
    /// A `String` representing the Redis key for the dead jobs set.
    pub fn dead_jobs_set(&self) -> String {
        DEAD_JOBS_SET.replace("{queue}", &self.namespace)
    }

    /// Returns the Redis key for the set of done jobs associated with the queue.
    /// The key is dynamically generated using the namespace of the queue.
    ///
    /// # Returns
    /// A `String` representing the Redis key for the done jobs set.
    pub fn done_jobs_set(&self) -> String {
        DONE_JOBS_SET.replace("{queue}", &self.namespace)
    }

    /// Returns the Redis key for the set of failed jobs associated with the queue.
    /// The key is dynamically generated using the namespace of the queue.
    ///
    /// # Returns
    /// A `String` representing the Redis key for the failed jobs set.
    pub fn failed_jobs_set(&self) -> String {
        FAILED_JOBS_SET.replace("{queue}", &self.namespace)
    }

    /// Returns the Redis key for the set of inflight jobs associated with the queue.
    /// The key is dynamically generated using the namespace of the queue.
    ///
    /// # Returns
    /// A `String` representing the Redis key for the inflight jobs set.
    pub fn inflight_jobs_set(&self) -> String {
        INFLIGHT_JOB_SET.replace("{queue}", &self.namespace)
    }

    /// Returns the Redis key for the hash storing job data associated with the queue.
    /// The key is dynamically generated using the namespace of the queue.
    ///
    /// # Returns
    /// A `String` representing the Redis key for the job data hash.
    pub fn job_data_hash(&self) -> String {
        JOB_DATA_HASH.replace("{queue}", &self.namespace)
    }

    /// Returns the Redis key for the set of scheduled jobs associated with the queue.
    /// The key is dynamically generated using the namespace of the queue.
    ///
    /// # Returns
    /// A `String` representing the Redis key for the scheduled jobs set.
    pub fn scheduled_jobs_set(&self) -> String {
        SCHEDULED_JOBS_SET.replace("{queue}", &self.namespace)
    }

    /// Returns the Redis key for the list of signals associated with the queue.
    /// The key is dynamically generated using the namespace of the queue.
    ///
    /// # Returns
    /// A `String` representing the Redis key for the signal list.
    pub fn signal_list(&self) -> String {
        SIGNAL_LIST.replace("{queue}", &self.namespace)
    }

    /// Gets the reenqueue_orphaned_after duration.
    pub fn reenqueue_orphaned_after(&self) -> Duration {
        self.reenqueue_orphaned_after
    }

    /// Gets a mutable reference to the reenqueue_orphaned_after.
    pub fn reenqueue_orphaned_after_mut(&mut self) -> &mut Duration {
        &mut self.reenqueue_orphaned_after
    }

    /// Occasionally some workers die, or abandon jobs because of panics.
    /// This is the time a task takes before its back to the queue
    ///
    /// Defaults to 5 minutes
    pub fn set_reenqueue_orphaned_after(mut self, after: Duration) -> Self {
        self.reenqueue_orphaned_after = after;
        self
    }
}

/// Represents a [Storage] that uses Redis for storage.
pub struct RedisStorage<T, Conn = ConnectionManager, C = JsonCodec<Vec<u8>>> {
    conn: Conn,
    job_type: PhantomData<T>,
    pub(super) scripts: RedisScript,
    controller: Controller,
    config: Config,
    codec: PhantomData<C>,
}

impl<T, Conn, C> fmt::Debug for RedisStorage<T, Conn, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisStorage")
            .field("conn", &"ConnectionManager")
            .field("job_type", &std::any::type_name::<T>())
            .field("scripts", &self.scripts)
            .field("config", &self.config)
            .finish()
    }
}

impl<T, Conn: Clone, C> Clone for RedisStorage<T, Conn, C> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            job_type: PhantomData,
            scripts: self.scripts.clone(),
            controller: self.controller.clone(),
            config: self.config.clone(),
            codec: self.codec,
        }
    }
}

impl<T: Serialize + DeserializeOwned, Conn> RedisStorage<T, Conn, JsonCodec<Vec<u8>>> {
    /// Start a new connection
    pub fn new(conn: Conn) -> RedisStorage<T, Conn, JsonCodec<Vec<u8>>> {
        Self::new_with_codec::<JsonCodec<Vec<u8>>>(
            conn,
            Config::default().set_namespace(type_name::<T>()),
        )
    }

    /// Start a connection with a custom config
    pub fn new_with_config(
        conn: Conn,
        config: Config,
    ) -> RedisStorage<T, Conn, JsonCodec<Vec<u8>>> {
        Self::new_with_codec::<JsonCodec<Vec<u8>>>(conn, config)
    }

    /// Start a new connection providing custom config and a codec
    pub fn new_with_codec<K>(conn: Conn, config: Config) -> RedisStorage<T, Conn, K>
    where
        K: Codec + Sync + Send + 'static,
    {
        RedisStorage {
            conn,
            job_type: PhantomData,
            controller: Controller::new(),
            config,
            codec: PhantomData::<K>,
            scripts: RedisScript {
                done_job: redis::Script::new(include_str!("../lua/done_job.lua")),
                push_job: redis::Script::new(include_str!("../lua/push_job.lua")),
                retry_job: redis::Script::new(include_str!("../lua/retry_job.lua")),
                enqueue_scheduled: redis::Script::new(include_str!(
                    "../lua/enqueue_scheduled_jobs.lua"
                )),
                get_jobs: redis::Script::new(include_str!("../lua/get_jobs.lua")),
                register_consumer: redis::Script::new(include_str!("../lua/register_consumer.lua")),
                kill_job: redis::Script::new(include_str!("../lua/kill_job.lua")),
                reenqueue_active: redis::Script::new(include_str!(
                    "../lua/reenqueue_active_jobs.lua"
                )),
                reenqueue_orphaned: redis::Script::new(include_str!(
                    "../lua/reenqueue_orphaned_jobs.lua"
                )),
                schedule_job: redis::Script::new(include_str!("../lua/schedule_job.lua")),
                vacuum: redis::Script::new(include_str!("../lua/vacuum.lua")),
                stats: redis::Script::new(include_str!("../lua/stats.lua")),
            },
        }
    }

    /// Get current connection
    pub fn get_connection(&self) -> &Conn {
        &self.conn
    }

    /// Get the config used by the storage
    pub fn get_config(&self) -> &Config {
        &self.config
    }
}

impl<T, Conn, C> RedisStorage<T, Conn, C> {
    /// Get the underlying codec details
    pub fn get_codec(&self) -> &PhantomData<C> {
        &self.codec
    }
}

impl<T, Conn, C> Backend<Request<T, RedisContext>> for RedisStorage<T, Conn, C>
where
    T: Serialize + DeserializeOwned + Sync + Send + Unpin + 'static,
    Conn: ConnectionLike + Send + Sync + 'static,
    C: Codec<Compact = Vec<u8>> + Send + 'static,
{
    type Stream = BackendStream<RequestStream<Request<T, RedisContext>>>;

    type Layer = AckLayer<Sender<(RedisContext, Response<Vec<u8>>)>, T, RedisContext, C>;

    type Codec = C;

    fn poll(
        mut self,
        worker: &Worker<apalis_core::worker::Context>,
    ) -> Poller<Self::Stream, Self::Layer> {
        let (mut tx, rx) = mpsc::channel(self.config.buffer_size);
        let (ack, ack_rx) =
            mpsc::channel::<(RedisContext, Response<Vec<u8>>)>(self.config.buffer_size);
        let layer = AckLayer::new(ack);
        let controller = self.controller.clone();
        let config = self.config.clone();
        let stream: RequestStream<Request<T, RedisContext>> = Box::pin(rx);
        let worker = worker.clone();
        let heartbeat = async move {
            // Lets reenqueue any jobs that belonged to this worker in case of a death
            if let Err(e) = self
                .reenqueue_orphaned((config.buffer_size * 10) as i32, Utc::now())
                .await
            {
                worker.emit(Event::Error(Box::new(
                    RedisPollError::ReenqueueOrphanedError(e),
                )));
            }

            let mut reenqueue_orphaned_stm =
                apalis_core::interval::interval(config.poll_interval).fuse();

            let mut keep_alive_stm = apalis_core::interval::interval(config.keep_alive).fuse();

            let mut enqueue_scheduled_stm =
                apalis_core::interval::interval(config.enqueue_scheduled).fuse();

            let mut poll_next_stm = apalis_core::interval::interval(config.poll_interval).fuse();

            let mut ack_stream = ack_rx.fuse();

            if let Err(e) = self.keep_alive(worker.id()).await {
                worker.emit(Event::Error(Box::new(RedisPollError::KeepAliveError(e))));
            }

            loop {
                select! {
                    _ = keep_alive_stm.next() => {
                        if let Err(e) = self.keep_alive(worker.id()).await {
                            worker.emit(Event::Error(Box::new(RedisPollError::KeepAliveError(e))));
                        }
                    }
                    _ = enqueue_scheduled_stm.next() => {
                        if let Err(e) = self.enqueue_scheduled(config.buffer_size).await {
                            worker.emit(Event::Error(Box::new(RedisPollError::EnqueueScheduledError(e))));
                        }
                    }
                    _ = poll_next_stm.next() => {
                        if worker.is_ready() {
                            let res = self.fetch_next(worker.id()).await;
                            match res {
                                Err(e) => {
                                    worker.emit(Event::Error(Box::new(RedisPollError::PollNextError(e))));
                                }
                                Ok(res) => {
                                    for job in res {
                                        if let Err(e) = tx.send(Ok(Some(job))).await {
                                            worker.emit(Event::Error(Box::new(RedisPollError::EnqueueError(e))));
                                        }
                                    }
                                }
                            }
                        } else {
                            continue;
                        }

                    }
                    id_to_ack = ack_stream.next() => {
                        if let Some((ctx, res)) = id_to_ack {
                            if let Err(e) = self.ack(&ctx, &res).await {
                                worker.emit(Event::Error(Box::new(RedisPollError::AckError(e))));
                            }
                        }
                    }
                    _ = reenqueue_orphaned_stm.next() => {
                        let dead_since = Utc::now()
                            - chrono::Duration::from_std(config.reenqueue_orphaned_after).unwrap();
                        if let Err(e) = self.reenqueue_orphaned((config.buffer_size * 10) as i32, dead_since).await {
                            worker.emit(Event::Error(Box::new(RedisPollError::ReenqueueOrphanedError(e))));
                        }
                    }
                };
            }
        };
        Poller::new_with_layer(
            BackendStream::new(stream, controller),
            heartbeat.boxed(),
            layer,
        )
    }
}

impl<T, Conn, C, Res> Ack<T, Res, C> for RedisStorage<T, Conn, C>
where
    T: Sync + Send + Serialize + DeserializeOwned + Unpin + 'static,
    Conn: ConnectionLike + Send + Sync + 'static,
    C: Codec<Compact = Vec<u8>> + Send + 'static,
    Res: Serialize + Sync + Send + 'static,
{
    type Context = RedisContext;
    type AckError = RedisError;
    async fn ack(&mut self, ctx: &Self::Context, res: &Response<Res>) -> Result<(), RedisError> {
        // Lets update the number of attempts
        // TODO: move attempts to its own key
        let mut task = self
            .fetch_by_id(&res.task_id)
            .await?
            .expect("must be a valid task");
        task.parts.attempt = res.attempt.clone();
        self.update(task).await?;
        // End of expensive update

        let inflight_set = format!(
            "{}:{}",
            self.config.inflight_jobs_set(),
            ctx.lock_by.clone().unwrap()
        );

        let now: i64 = Utc::now().timestamp();
        let task_id = res.task_id.to_string();
        match &res.inner {
            Ok(success_res) => {
                let done_job = self.scripts.done_job.clone();
                let done_jobs_set = &self.config.done_jobs_set();
                done_job
                    .key(inflight_set)
                    .key(done_jobs_set)
                    .key(self.config.job_data_hash())
                    .arg(task_id)
                    .arg(now)
                    .arg(C::encode(success_res).map_err(Into::into).unwrap())
                    .invoke_async(&mut self.conn)
                    .await
            }
            Err(e) => match e {
                Error::Abort(e) => {
                    let worker_id = ctx.lock_by.as_ref().unwrap();
                    self.kill(worker_id, &res.task_id, &e).await
                }
                _ => {
                    if ctx.max_attempts > res.attempt.current() {
                        let worker_id = ctx.lock_by.as_ref().unwrap();
                        self.retry(worker_id, &res.task_id).await.map(|_| ())
                    } else {
                        let worker_id = ctx.lock_by.as_ref().unwrap();

                        self.kill(
                            worker_id,
                            &res.task_id,
                            &(Box::new(io::Error::new(
                                io::ErrorKind::Interrupted,
                                format!("Max retries of {} exceeded", ctx.max_attempts),
                            )) as BoxDynError),
                        )
                        .await
                    }
                }
            },
        }
    }
}

impl<T, Conn, C> RedisStorage<T, Conn, C>
where
    T: DeserializeOwned + Send + Unpin + Send + Sync + 'static,
    Conn: ConnectionLike + Send + Sync + 'static,
    C: Codec<Compact = Vec<u8>>,
{
    async fn fetch_next(
        &mut self,
        worker_id: &WorkerId,
    ) -> Result<Vec<Request<T, RedisContext>>, RedisError> {
        let fetch_jobs = self.scripts.get_jobs.clone();
        let consumers_set = self.config.consumers_set();
        let active_jobs_list = self.config.active_jobs_list();
        let job_data_hash = self.config.job_data_hash();
        let inflight_set = format!("{}:{}", self.config.inflight_jobs_set(), worker_id);
        let signal_list = self.config.signal_list();
        let namespace = &self.config.namespace;

        let result = fetch_jobs
            .key(&consumers_set)
            .key(&active_jobs_list)
            .key(&inflight_set)
            .key(&job_data_hash)
            .key(&signal_list)
            .arg(self.config.buffer_size) // No of jobs to fetch
            .arg(&inflight_set)
            .invoke_async::<Vec<Value>>(&mut self.conn)
            .await;

        match result {
            Ok(jobs) => {
                let mut processed = vec![];
                for job in jobs {
                    let bytes = deserialize_job(&job)?;
                    let mut request: Request<T, RedisContext> =
                        C::decode(bytes.clone()).map_err(|e| build_error(&e.into().to_string()))?;
                    request.parts.context.lock_by = Some(worker_id.clone());
                    request.parts.namespace = Some(Namespace(namespace.clone()));
                    processed.push(request)
                }
                Ok(processed)
            }
            Err(e) => {
                warn!("An error occurred during streaming jobs: {e}");
                if matches!(e.kind(), ErrorKind::ResponseError)
                    && e.to_string().contains("consumer not registered script")
                {
                    self.keep_alive(worker_id).await?;
                }
                Err(e)
            }
        }
    }
}

fn build_error(message: &str) -> RedisError {
    RedisError::from(io::Error::new(io::ErrorKind::InvalidData, message))
}

fn deserialize_job(job: &Value) -> Result<&Vec<u8>, RedisError> {
    match job {
        Value::BulkString(bytes) => Ok(bytes),
        Value::Array(val) | Value::Set(val) => val
            .first()
            .and_then(|val| {
                if let Value::BulkString(bytes) = val {
                    Some(bytes)
                } else {
                    None
                }
            })
            .ok_or(build_error("Value::Bulk: Invalid data returned by storage")),
        _ => Err(build_error("unknown result type for next message")),
    }
}

impl<T, Conn: ConnectionLike, C> RedisStorage<T, Conn, C> {
    async fn keep_alive(&mut self, worker_id: &WorkerId) -> Result<(), RedisError> {
        let register_consumer = self.scripts.register_consumer.clone();
        let inflight_set = format!("{}:{}", self.config.inflight_jobs_set(), worker_id);
        let consumers_set = self.config.consumers_set();

        let now: i64 = Utc::now().timestamp();

        register_consumer
            .key(consumers_set)
            .arg(now)
            .arg(inflight_set)
            .invoke_async(&mut self.conn)
            .await
    }
}

impl<T, Conn, C> Storage for RedisStorage<T, Conn, C>
where
    T: Serialize + DeserializeOwned + Send + 'static + Unpin + Sync,
    Conn: ConnectionLike + Send + Sync + 'static,
    C: Codec<Compact = Vec<u8>> + Send + 'static,
{
    type Job = T;
    type Error = RedisError;
    type Context = RedisContext;

    type Compact = Vec<u8>;

    async fn push_request(
        &mut self,
        req: Request<T, RedisContext>,
    ) -> Result<Parts<Self::Context>, RedisError> {
        let conn = &mut self.conn;
        let push_job = self.scripts.push_job.clone();
        let job_data_hash = self.config.job_data_hash();
        let active_jobs_list = self.config.active_jobs_list();
        let signal_list = self.config.signal_list();

        let job = C::encode(&req)
            .map_err(|e| (ErrorKind::IoError, "Encode error", e.into().to_string()))?;
        push_job
            .key(job_data_hash)
            .key(active_jobs_list)
            .key(signal_list)
            .arg(req.parts.task_id.to_string())
            .arg(job)
            .invoke_async(conn)
            .await?;
        Ok(req.parts)
    }

    async fn push_raw_request(
        &mut self,
        req: Request<Self::Compact, Self::Context>,
    ) -> Result<Parts<Self::Context>, Self::Error> {
        let conn = &mut self.conn;
        let push_job = self.scripts.push_job.clone();
        let job_data_hash = self.config.job_data_hash();
        let active_jobs_list = self.config.active_jobs_list();
        let signal_list = self.config.signal_list();

        let job = C::encode(&req)
            .map_err(|e| (ErrorKind::IoError, "Encode error", e.into().to_string()))?;
        push_job
            .key(job_data_hash)
            .key(active_jobs_list)
            .key(signal_list)
            .arg(req.parts.task_id.to_string())
            .arg(job)
            .invoke_async(conn)
            .await?;
        Ok(req.parts)
    }

    async fn schedule_request(
        &mut self,
        req: Request<Self::Job, RedisContext>,
        on: i64,
    ) -> Result<Parts<Self::Context>, RedisError> {
        let schedule_job = self.scripts.schedule_job.clone();
        let job_data_hash = self.config.job_data_hash();
        let scheduled_jobs_set = self.config.scheduled_jobs_set();
        let job = C::encode(&req)
            .map_err(|e| (ErrorKind::IoError, "Encode error", e.into().to_string()))?;
        schedule_job
            .key(job_data_hash)
            .key(scheduled_jobs_set)
            .arg(req.parts.task_id.to_string())
            .arg(job)
            .arg(on)
            .invoke_async(&mut self.conn)
            .await?;
        Ok(req.parts)
    }

    async fn len(&mut self) -> Result<i64, RedisError> {
        let pending_jobs: i64 = redis::cmd("LLEN")
            .arg(self.config.active_jobs_list())
            .query_async(&mut self.conn)
            .await?;

        Ok(pending_jobs)
    }

    async fn fetch_by_id(
        &mut self,
        job_id: &TaskId,
    ) -> Result<Option<Request<Self::Job, RedisContext>>, RedisError> {
        let data: Value = redis::cmd("HMGET")
            .arg(self.config.job_data_hash())
            .arg(job_id.to_string())
            .query_async(&mut self.conn)
            .await?;
        let bytes = deserialize_job(&data)?;

        let inner: Request<T, RedisContext> = C::decode(bytes.to_vec())
            .map_err(|e| (ErrorKind::IoError, "Decode error", e.into().to_string()))?;
        Ok(Some(inner))
    }
    async fn update(&mut self, job: Request<T, RedisContext>) -> Result<(), RedisError> {
        let task_id = job.parts.task_id.to_string();
        let bytes = C::encode(&job)
            .map_err(|e| (ErrorKind::IoError, "Encode error", e.into().to_string()))?;
        let _: i64 = redis::cmd("HSET")
            .arg(self.config.job_data_hash())
            .arg(task_id)
            .arg(bytes)
            .query_async(&mut self.conn)
            .await?;
        Ok(())
    }

    async fn reschedule(
        &mut self,
        job: Request<T, RedisContext>,
        wait: Duration,
    ) -> Result<(), RedisError> {
        let schedule_job = self.scripts.schedule_job.clone();
        let job_id = &job.parts.task_id;
        let worker_id = &job.parts.context.lock_by.clone().unwrap();
        let job = C::encode(&job)
            .map_err(|e| (ErrorKind::IoError, "Encode error", e.into().to_string()))?;
        let job_data_hash = self.config.job_data_hash();
        let scheduled_jobs_set = self.config.scheduled_jobs_set();
        let on: i64 = Utc::now().timestamp();
        let wait: i64 = wait
            .as_secs()
            .try_into()
            .map_err(|e: TryFromIntError| (ErrorKind::IoError, "Duration error", e.to_string()))?;
        let inflight_set = format!("{}:{}", self.config.inflight_jobs_set(), worker_id);
        let failed_jobs_set = self.config.failed_jobs_set();
        redis::cmd("SREM")
            .arg(inflight_set)
            .arg(job_id.to_string())
            .query_async(&mut self.conn)
            .await?;
        redis::cmd("ZADD")
            .arg(failed_jobs_set)
            .arg(on)
            .arg(job_id.to_string())
            .query_async(&mut self.conn)
            .await?;
        schedule_job
            .key(job_data_hash)
            .key(scheduled_jobs_set)
            .arg(job_id.to_string())
            .arg(job)
            .arg(on + wait)
            .invoke_async(&mut self.conn)
            .await
    }
    async fn is_empty(&mut self) -> Result<bool, RedisError> {
        self.len().map_ok(|res| res == 0).await
    }

    async fn vacuum(&mut self) -> Result<usize, RedisError> {
        let vacuum_script = self.scripts.vacuum.clone();
        vacuum_script
            .key(self.config.done_jobs_set())
            .key(self.config.job_data_hash())
            .invoke_async(&mut self.conn)
            .await
    }
}

impl<T, Conn, C> RedisStorage<T, Conn, C>
where
    Conn: ConnectionLike + Send + Sync + 'static,
    C: Codec<Compact = Vec<u8>> + Send + 'static,
{
    /// Attempt to retry a job
    pub async fn retry(&mut self, worker_id: &WorkerId, task_id: &TaskId) -> Result<i32, RedisError>
    where
        T: Send + DeserializeOwned + Serialize + Unpin + Sync + 'static,
    {
        let retry_job = self.scripts.retry_job.clone();
        let inflight_set = format!("{}:{}", self.config.inflight_jobs_set(), worker_id);
        let scheduled_jobs_set = self.config.scheduled_jobs_set();
        let job_data_hash = self.config.job_data_hash();
        let job_fut = self.fetch_by_id(task_id);
        let now: i64 = Utc::now().timestamp();
        let res = job_fut.await?;
        let conn = &mut self.conn;
        match res {
            Some(job) => {
                let attempt = &job.parts.attempt;
                let max_attempts = &job.parts.context.max_attempts;
                if &attempt.current() >= &max_attempts {
                    self.kill(
                        worker_id,
                        task_id,
                        &(Box::new(io::Error::new(
                            io::ErrorKind::Interrupted,
                            format!("Max retries of {} exceeded", max_attempts),
                        )) as BoxDynError),
                    )
                    .await?;
                    return Ok(1);
                }
                let job = C::encode(job)
                    .map_err(|e| (ErrorKind::IoError, "Encode error", e.into().to_string()))?;

                let res: Result<i32, RedisError> = retry_job
                    .key(inflight_set)
                    .key(scheduled_jobs_set)
                    .key(job_data_hash)
                    .arg(task_id.to_string())
                    .arg(now)
                    .arg(job)
                    .invoke_async(conn)
                    .await;
                match res {
                    Ok(count) => Ok(count),
                    Err(e) => Err(e),
                }
            }
            None => Err(RedisError::from((ErrorKind::ResponseError, "Id not found"))),
        }
    }

    /// Attempt to kill a job
    pub async fn kill(
        &mut self,
        worker_id: &WorkerId,
        task_id: &TaskId,
        error: &BoxDynError,
    ) -> Result<(), RedisError> {
        let kill_job = self.scripts.kill_job.clone();
        let current_worker_id = format!("{}:{}", self.config.inflight_jobs_set(), worker_id);
        let job_data_hash = self.config.job_data_hash();
        let dead_jobs_set = self.config.dead_jobs_set();
        let now: i64 = Utc::now().timestamp();
        kill_job
            .key(current_worker_id)
            .key(dead_jobs_set)
            .key(job_data_hash)
            .arg(task_id.to_string())
            .arg(now)
            .arg(error.to_string())
            .invoke_async(&mut self.conn)
            .await
    }

    /// Required to add scheduled jobs to the active set
    pub async fn enqueue_scheduled(&mut self, count: usize) -> Result<usize, RedisError> {
        let enqueue_jobs = self.scripts.enqueue_scheduled.clone();
        let scheduled_jobs_set = self.config.scheduled_jobs_set();
        let active_jobs_list = self.config.active_jobs_list();
        let signal_list = self.config.signal_list();
        let now: i64 = Utc::now().timestamp();
        let res: Result<usize, _> = enqueue_jobs
            .key(scheduled_jobs_set)
            .key(active_jobs_list)
            .key(signal_list)
            .arg(now)
            .arg(count)
            .invoke_async(&mut self.conn)
            .await;
        match res {
            Ok(count) => Ok(count),
            Err(e) => Err(e),
        }
    }

    /// Re-enqueue some jobs that might be abandoned.
    pub async fn reenqueue_active(&mut self, job_ids: Vec<&TaskId>) -> Result<(), RedisError> {
        let reenqueue_active = self.scripts.reenqueue_active.clone();
        let inflight_set: String = self.config.inflight_jobs_set().to_string();
        let active_jobs_list = self.config.active_jobs_list();
        let signal_list = self.config.signal_list();

        reenqueue_active
            .key(inflight_set)
            .key(active_jobs_list)
            .key(signal_list)
            .arg(
                job_ids
                    .into_iter()
                    .map(|j| j.to_string())
                    .collect::<Vec<String>>(),
            )
            .invoke_async(&mut self.conn)
            .await
    }
    /// Re-enqueue some jobs that might be orphaned after a number of seconds
    pub async fn reenqueue_orphaned(
        &mut self,
        count: i32,
        dead_since: DateTime<Utc>,
    ) -> Result<usize, RedisError> {
        let reenqueue_orphaned = self.scripts.reenqueue_orphaned.clone();
        let consumers_set = self.config.consumers_set();
        let active_jobs_list = self.config.active_jobs_list();
        let signal_list = self.config.signal_list();

        let dead_since = dead_since.timestamp();

        let res: Result<usize, RedisError> = reenqueue_orphaned
            .key(consumers_set)
            .key(active_jobs_list)
            .key(signal_list)
            .arg(dead_since)
            .arg(count)
            .invoke_async(&mut self.conn)
            .await;
        match res {
            Ok(count) => Ok(count),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use apalis_core::worker::Context;
    use apalis_core::{generic_storage_test, sleep};
    use email_service::Email;

    use apalis_core::test_utils::apalis_test_service_fn;
    use apalis_core::test_utils::TestWrapper;

    generic_storage_test!(setup);

    use super::*;

    /// migrate DB and return a storage instance.
    async fn setup<T: Serialize + DeserializeOwned>() -> RedisStorage<T> {
        let redis_url = std::env::var("REDIS_URL").expect("No REDIS_URL is specified");
        // Because connections cannot be shared across async runtime
        // (different runtimes are created for each test),
        // we don't share the storage and tests must be run sequentially.
        let conn = connect(redis_url).await.unwrap();
        let config = Config::default()
            .set_namespace("apalis::test")
            .set_enqueue_scheduled(Duration::from_millis(500)); // Instantly return jobs to the queue
        let mut storage = RedisStorage::new_with_config(conn, config);
        cleanup(&mut storage, &WorkerId::new("test-worker")).await;
        storage
    }

    /// rollback DB changes made by tests.
    ///
    /// You should execute this function in the end of a test
    async fn cleanup<T>(storage: &mut RedisStorage<T>, _worker_id: &WorkerId) {
        let _resp: String = redis::cmd("FLUSHDB")
            .query_async(&mut storage.conn)
            .await
            .expect("failed to Flushdb");
    }

    fn example_email() -> Email {
        Email {
            subject: "Test Subject".to_string(),
            to: "example@postgres".to_string(),
            text: "Some Text".to_string(),
        }
    }

    async fn consume_one(
        storage: &mut RedisStorage<Email>,
        worker_id: &WorkerId,
    ) -> Request<Email, RedisContext> {
        let stream = storage.fetch_next(worker_id);
        stream
            .await
            .expect("failed to poll job")
            .first()
            .expect("stream is empty")
            .clone()
    }

    async fn register_worker_at(storage: &mut RedisStorage<Email>) -> Worker<Context> {
        let worker = Worker::new(WorkerId::new("test-worker"), Context::default());
        worker.start();
        storage
            .keep_alive(&worker.id())
            .await
            .expect("failed to register worker");
        worker
    }

    async fn register_worker(storage: &mut RedisStorage<Email>) -> Worker<Context> {
        register_worker_at(storage).await
    }

    async fn push_email(storage: &mut RedisStorage<Email>, email: Email) {
        storage.push(email).await.expect("failed to push a job");
    }

    async fn get_job(
        storage: &mut RedisStorage<Email>,
        job_id: &TaskId,
    ) -> Request<Email, RedisContext> {
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

        let _job = consume_one(&mut storage, &worker.id()).await;
    }

    #[tokio::test]
    async fn test_acknowledge_job() {
        let mut storage = setup().await;
        push_email(&mut storage, example_email()).await;

        let worker = register_worker(&mut storage).await;

        let job = consume_one(&mut storage, &worker.id()).await;
        let ctx = &job.parts.context;
        let res = 42usize;
        storage
            .ack(
                ctx,
                &Response::success(res, job.parts.task_id.clone(), job.parts.attempt.clone()),
            )
            .await
            .expect("failed to acknowledge the job");

        let _job = get_job(&mut storage, &job.parts.task_id).await;
    }

    #[tokio::test]
    async fn test_kill_job() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let worker = register_worker(&mut storage).await;

        let job = consume_one(&mut storage, &worker.id()).await;
        let job_id = &job.parts.task_id;

        storage
            .kill(
                &worker.id(),
                &job_id,
                &(Box::new(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "Some unforeseen error occurred",
                )) as BoxDynError),
            )
            .await
            .expect("failed to kill job");

        let _job = get_job(&mut storage, &job_id).await;
    }

    #[tokio::test]
    async fn test_heartbeat_renqueueorphaned_pulse_last_seen_1sec() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let worker = register_worker_at(&mut storage).await;

        let job = consume_one(&mut storage, &worker.id()).await;
        sleep(Duration::from_millis(1000)).await;
        let dead_since = Utc::now() - chrono::Duration::from_std(Duration::from_secs(1)).unwrap();
        let res = storage
            .reenqueue_orphaned(1, dead_since)
            .await
            .expect("failed to reenqueue_orphaned");
        // We expect 1 job to be re-enqueued
        assert_eq!(res, 1);
        let job = get_job(&mut storage, &job.parts.task_id).await;
        let ctx = &job.parts.context;
        // assert_eq!(*ctx.status(), State::Pending);
        // assert!(ctx.done_at().is_none());
        assert!(ctx.lock_by.is_none());
        // assert!(ctx.lock_at().is_none());
        // assert_eq!(*ctx.last_error(), Some("Job was abandoned".to_owned()));
        // TODO: Redis should store context aside
        // assert_eq!(job.parts.attempt.current(), 1);
    }

    #[tokio::test]
    async fn test_heartbeat_renqueueorphaned_pulse_last_seen_5sec() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let worker = register_worker_at(&mut storage).await;
        sleep(Duration::from_millis(1100)).await;
        let job = consume_one(&mut storage, &worker.id()).await;
        let dead_since = Utc::now() - chrono::Duration::from_std(Duration::from_secs(5)).unwrap();
        let res = storage
            .reenqueue_orphaned(1, dead_since)
            .await
            .expect("failed to reenqueue_orphaned");
        // We expect 0 job to be re-enqueued
        assert_eq!(res, 0);
        let job = get_job(&mut storage, &job.parts.task_id).await;
        let _ctx = &job.parts.context;
        // assert_eq!(*ctx.status(), State::Running);
        // TODO: update redis context
        // assert_eq!(ctx.lock_by, Some(worker_id));
        // assert!(ctx.lock_at().is_some());
        // assert_eq!(*ctx.last_error(), None);
        assert_eq!(job.parts.attempt.current(), 0);
    }

    #[tokio::test]
    async fn test_stats() {
        use apalis_core::backend::BackendExpose;

        let mut storage = setup().await;
        let stats = storage.stats().await.expect("failed to get stats");
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.running, 0);
        push_email(&mut storage, example_email()).await;
        let stats = storage.stats().await.expect("failed to get stats");
        assert_eq!(stats.pending, 1);

        let worker = register_worker(&mut storage).await;

        let _job = consume_one(&mut storage, &worker.id()).await;

        let stats = storage.stats().await.expect("failed to get stats");
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.running, 1);
    }
}
