#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! apalis storage using Redis as a backend
//! ```rust,no_run
//! use apalis::prelude::*;
//! use apalis_redis::{RedisStorage, Config};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Deserialize, Serialize)]
//! struct Email {
//!     to: String,
//! }
//!
//! async fn send_email(job: Email) -> Result<(), Error> {
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let redis_url = std::env::var("REDIS_URL").expect("Missing env variable REDIS_URL");
//!     let conn = apalis_redis::connect(redis_url).await.expect("Could not connect");
//!     let storage = RedisStorage::new(conn);
//!     let worker = WorkerBuilder::new("tasty-pear")
//!         .backend(storage.clone())
//!         .build_fn(send_email);
//!
//!     worker.run().await;
//! }
//! ```

use std::{
    any::type_name,
    collections::HashMap,
    convert::Infallible,
    future::Future,
    io,
    marker::PhantomData,
    pin::Pin,
    str::FromStr,
    sync::{Arc, LazyLock, Mutex, OnceLock},
    task::{Context, Poll},
    time::{Duration, SystemTime},
    usize,
};

use apalis_core::{
    backend::{
        codec::{json::JsonCodec, Codec, Decoder, Encoder},
        shared::MakeShared,
        Backend, RequestStream, TaskSink,
    },
    error::BoxDynError,
    request::{attempt::Attempt, state::Status, task_id::TaskId, Parts, Request},
    service_fn::from_request::FromRequest,
    worker::{
        context::WorkerContext,
        ext::ack::{Acknowledge, AcknowledgeLayer},
    },
    workflow::GoTo,
};
use chrono::Utc;
use event_listener::Event;
use futures::{
    future::{select, BoxFuture},
    stream::{self, BoxStream},
    FutureExt, Sink, StreamExt, TryFuture,
};
use redis::{
    aio::{ConnectionLike, MultiplexedConnection},
    AsyncConnectionConfig, Client, ErrorKind, PushInfo, Script, ScriptInvocation, Value,
};
// mod expose;
// mod storage;
pub use redis::{aio::ConnectionManager, RedisError};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tower::{layer::util::Identity, ServiceBuilder};
// pub use storage::connect;
// pub use storage::Config;
// pub use storage::RedisContext;
// pub use storage::RedisPollError;
// pub use storage::RedisQueueInfo;
// pub use storage::RedisStorage;
const ACTIVE_JOBS_LIST: &str = "{queue}:active";
const CONSUMERS_SET: &str = "{queue}:consumers";
const DEAD_JOBS_SET: &str = "{queue}:dead";
const DONE_JOBS_SET: &str = "{queue}:done";
const FAILED_JOBS_SET: &str = "{queue}:failed";
const INFLIGHT_JOB_SET: &str = "{queue}:inflight";
const JOB_DATA_HASH: &str = "{queue}:data";
const JOB_META_HASH: &str = "{queue}:meta";
const SCHEDULED_JOBS_SET: &str = "{queue}:scheduled";
const SIGNAL_LIST: &str = "{queue}:signal";

/// The context for a redis storage job
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RedisContext {
    max_attempts: u32,
    lock_by: Option<String>,
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

impl<Req: Sync> FromRequest<Request<Req, RedisContext>> for RedisContext {
    type Error = Infallible;
    async fn from_request(req: &Request<Req, RedisContext>) -> Result<Self, Self::Error> {
        Ok(req.parts.context.clone())
    }
}

/// Config for a [RedisStorage]
#[derive(Clone, Debug)]
pub struct RedisConfig {
    poll_interval: Duration,
    buffer_size: usize,
    keep_alive: Duration,
    enqueue_scheduled: Duration,
    reenqueue_orphaned_after: Duration,
    namespace: String,
}

impl Default for RedisConfig {
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

/// Represents a [Backend] that uses Redis for storage.
#[doc = "# Feature Support\n"]
pub struct RedisStorage<Args, Conn = ConnectionManager, C = JsonCodec<Vec<u8>>> {
    conn: Conn,
    job_type: PhantomData<Args>,
    config: RedisConfig,
    codec: PhantomData<C>,
    poller: Arc<Event>,
}

impl<T, Conn> RedisStorage<T, Conn, JsonCodec<Vec<u8>>> {
    /// Start a new connection
    pub fn new(conn: Conn) -> RedisStorage<T, Conn, JsonCodec<Vec<u8>>> {
        Self::new_with_codec::<JsonCodec<Vec<u8>>>(
            conn,
            RedisConfig::default().set_namespace(type_name::<T>()),
        )
    }

    /// Start a connection with a custom config
    pub fn new_with_config(
        conn: Conn,
        config: RedisConfig,
    ) -> RedisStorage<T, Conn, JsonCodec<Vec<u8>>> {
        Self::new_with_codec::<JsonCodec<Vec<u8>>>(conn, config)
    }

    /// Start a new connection providing custom config and a codec
    pub fn new_with_codec<K>(conn: Conn, config: RedisConfig) -> RedisStorage<T, Conn, K>
    where
        K: Sync + Send + 'static,
    {
        RedisStorage {
            conn,
            job_type: PhantomData,
            config,
            codec: PhantomData::<K>,
            poller: Arc::new(Event::new()),
        }
    }

    /// Get current connection
    pub fn get_connection(&self) -> &Conn {
        &self.conn
    }

    /// Get the config used by the storage
    pub fn get_config(&self) -> &RedisConfig {
        &self.config
    }
}

impl<Args, Conn, C> Backend<Args, RedisContext> for RedisStorage<Args, Conn, C>
where
    Args: DeserializeOwned + Unpin + Send + Sync + 'static,
    Conn: Clone + ConnectionLike + Send + Sync + 'static,
    C: Decoder<Args, Compact = Vec<u8>> + Unpin + Send + 'static,
    C::Error: Into<BoxDynError>,
{
    type Stream = RequestStream<Request<Args, RedisContext>, RedisError>;

    type Error = RedisError;
    type Layer = AcknowledgeLayer<RedisAck<Conn>>;

    type Beat = BoxStream<'static, Result<(), Self::Error>>;
    type Sink = RedisSink<Args, C, Conn>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let keep_alive = self.config.keep_alive;

        let config = self.config.clone();
        let worker_id = worker.name().to_owned();
        let conn = self.conn.clone();

        let stream = stream::unfold(
            (keep_alive, worker_id, conn, config),
            |(keep_alive, worker_id, mut conn, config)| async move {
                apalis_core::timer::sleep(keep_alive).await;
                let register_consumer =
                    redis::Script::new(include_str!("../lua/register_consumer.lua"));
                let inflight_set = format!("{}:{}", config.inflight_jobs_set(), worker_id);
                let consumers_set = config.consumers_set();

                let now: i64 = Utc::now().timestamp();

                let res = register_consumer
                    .key(consumers_set)
                    .arg(now)
                    .arg(inflight_set)
                    .invoke_async::<()>(&mut conn)
                    .await;
                Some((res, (keep_alive, worker_id, conn, config)))
            },
        );
        stream.boxed()
    }
    fn middleware(&self) -> Self::Layer {
        AcknowledgeLayer::new(RedisAck {
            conn: self.conn.clone(),
            config: self.config.clone(),
        })
    }

    fn sink(&self) -> Self::Sink {
        RedisSink {
            _args: PhantomData,
            pending: Vec::new(),
            config: self.config.clone(),
            conn: self.conn.clone(),
            invoke_future: None,
        }
    }

    fn poll(mut self, worker: &WorkerContext) -> Self::Stream {
        let worker = worker.clone();
        let worker_id = worker.name().to_owned();
        let config = self.config.clone();
        let mut conn = self.conn.clone();
        let event_listener = self.poller.clone();
        let register = futures::stream::once(async move {
            let register_consumer =
                redis::Script::new(include_str!("../lua/register_consumer.lua"));
            let inflight_set = format!("{}:{}", config.inflight_jobs_set(), worker_id);
            let consumers_set = config.consumers_set();

            let now: i64 = Utc::now().timestamp();

            register_consumer
                .key(consumers_set)
                .arg(now)
                .arg(inflight_set)
                .invoke_async::<()>(&mut conn)
                .await?;
            Ok(None)
        })
        .filter_map(
            |res: Result<Option<Request<Args, RedisContext>>, RedisError>| async move {
                match res {
                    Ok(_) => None,
                    Err(e) => Some(Err(e)),
                }
            },
        );
        let stream = stream::unfold(
            (
                worker,
                self.config.clone(),
                self.conn.clone(),
                event_listener,
            ),
            |(worker, config, mut conn, event_listener)| async {
                let interval = apalis_core::timer::sleep(config.poll_interval).boxed();
                let pub_sub = event_listener.listen().boxed();
                select(pub_sub, interval).await; // Pubsub or else interval
                let data = Self::fetch_next(&worker, &config, &mut conn).await;
                Some((data, (worker, config, conn, event_listener)))
            },
        )
        .flat_map(|res| match res {
            Ok(s) => {
                let stm: Vec<_> = s
                    .into_iter()
                    .map(|s| Ok::<_, RedisError>(Some(s)))
                    .collect();
                stream::iter(stm)
            }
            Err(e) => stream::iter(vec![Err(e)]),
        });
        register.chain(stream).boxed()
        // stream
    }
}

impl<T, Conn, C> RedisStorage<T, Conn, C>
where
    T: DeserializeOwned + Unpin + Send + Sync + 'static,
    Conn: ConnectionLike + Send + Sync + 'static,
    C: Decoder<T, Compact = Vec<u8>>,
    C::Error: Into<BoxDynError>,
{
    async fn fetch_next(
        worker: &WorkerContext,
        config: &RedisConfig,
        conn: &mut Conn,
    ) -> Result<Vec<Request<T, RedisContext>>, RedisError> {
        let fetch_jobs = redis::Script::new(include_str!("../lua/get_jobs.lua"));
        let consumers_set = config.consumers_set();
        let active_jobs_list = config.active_jobs_list();
        let job_data_hash = config.job_data_hash();
        let inflight_set = format!("{}:{}", config.inflight_jobs_set(), worker.name());
        let signal_list = config.signal_list();

        let result = fetch_jobs
            .key(&consumers_set)
            .key(&active_jobs_list)
            .key(&inflight_set)
            .key(&job_data_hash)
            .key(&signal_list)
            .key(&config.job_meta_hash())
            .arg(config.buffer_size) // No of jobs to fetch
            .arg(&inflight_set)
            .invoke_async::<Vec<Value>>(&mut *conn)
            .await;
        match result {
            Ok(jobs) => {
                let mut processed = vec![];
                let tasks = deserialize_with_meta(&jobs[0], &jobs[1])?;
                for task in tasks {
                    let args: T =
                        C::decode(task.data).map_err(|e| build_error(&e.into().to_string()))?;
                    let context = RedisContext {
                        max_attempts: task.max_attempts,
                        ..Default::default()
                    };
                    let mut parts = Parts::default();
                    parts.attempt = Attempt::new_with_value(task.attempts as usize);
                    parts.context = context;
                    parts.status =
                        Status::from_str(task.status).map_err(|e| build_error(&e.to_string()))?;
                    let task_id =
                        TaskId::from_str(task.task_id).map_err(|e| build_error(&e.to_string()))?;

                    parts.task_id = task_id;
                    let mut request: Request<T, RedisContext> =
                        Request::new_with_parts(args, parts);
                    request.parts.context.lock_by = Some(worker.name().clone());
                    processed.push(request)
                }
                Ok(processed)
            }
            Err(e) => Err(e),
        }
    }
}

fn build_error(message: &str) -> RedisError {
    RedisError::from(io::Error::new(io::ErrorKind::InvalidData, message))
}

#[derive(Debug)]
struct TaskWithMeta<'a> {
    pub data: &'a Vec<u8>,
    pub attempts: u32,
    pub max_attempts: u32,
    pub status: &'a str,
    pub task_id: &'a str,
}

fn parse_u32(value: &Value, field: &str) -> Result<u32, RedisError> {
    match value {
        Value::BulkString(bytes) => {
            let s = std::str::from_utf8(bytes)
                .map_err(|_| build_error(&format!("{} not UTF-8", field)))?;
            s.parse::<u32>()
                .map_err(|_| build_error(&format!("{} not u32", field)))
        }
        _ => Err(build_error(&format!("{} not bulk string", field))),
    }
}

fn deserialize_with_meta<'a>(
    job_data_list: &'a redis::Value,
    meta_list: &'a redis::Value,
) -> Result<Vec<TaskWithMeta<'a>>, RedisError> {
    let job_data_list = match job_data_list {
        redis::Value::Array(vals) => vals,
        _ => return Err(build_error("Expected job_data to be array")),
    };

    let meta_list = match meta_list {
        redis::Value::Array(vals) => vals,
        _ => return Err(build_error("Expected metadata to be array")),
    };

    if job_data_list.len() != meta_list.len() {
        return Err(build_error("Job data and metadata length mismatch"));
    }

    let mut result = Vec::with_capacity(job_data_list.len());

    for (data_val, meta_val) in job_data_list.iter().zip(meta_list.iter()) {
        let data = match data_val {
            redis::Value::BulkString(bytes) => bytes,
            _ => return Err(build_error("Invalid job data format")),
        };

        let meta_fields = match meta_val {
            redis::Value::Array(fields) if fields.len() == 4 => fields,
            _ => return Err(build_error("Invalid metadata format")),
        };

        fn str_from_val<'a>(val: &'a redis::Value, field: &'a str) -> Result<&'a str, RedisError> {
            match val {
                redis::Value::BulkString(bytes) => std::str::from_utf8(bytes)
                    .map_err(|_| build_error(&format!("{} not UTF-8", field))),
                _ => Err(build_error(&format!("{} not bulk string", field))),
            }
        }

        let task_id = str_from_val(&meta_fields[0], "task_id")?;
        let attempts = parse_u32(&meta_fields[1], "attempts")?;
        let max_attempts = parse_u32(&meta_fields[2], "max_attempts")?;
        let status = str_from_val(&meta_fields[3], "status")?;

        result.push(TaskWithMeta {
            task_id,
            data,
            attempts,
            max_attempts,
            status,
        });
    }

    Ok(result)
}

pub struct SharedRedisStorage {
    conn: MultiplexedConnection,
    registry: Arc<Mutex<HashMap<String, Arc<Event>>>>,
}

fn parse_channel_info(push: &PushInfo) -> Option<(String, String, String)> {
    if let Some(Value::BulkString(channel_bytes)) = push.data.get(1) {
        if let Ok(channel_str) = std::str::from_utf8(channel_bytes) {
            let parts: Vec<&str> = channel_str.split(':').collect();
            if parts.len() >= 4 {
                let namespace = parts[1].to_owned();
                let action = parts[2].to_owned();
                let signal = parts[3].to_string();
                return Some((namespace, action, signal));
            }
        }
    }
    None
}

impl SharedRedisStorage {
    pub async fn new(client: Client) -> Result<Self, RedisError> {
        let registry: Arc<Mutex<HashMap<String, Arc<Event>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let r2 = registry.clone();
        let config = AsyncConnectionConfig::new().set_push_sender(move |msg| {
            let Ok(mut registry) = r2.lock() else {
                return Err(redis::aio::SendError);
            };
            if let Some((namespace, _, signal_kind)) = parse_channel_info(&msg) {
                if signal_kind == "available" {
                    registry.get(&namespace).unwrap().notify(usize::MAX);
                }
            }
            Ok(())
        });
        let mut conn = client
            .get_multiplexed_async_connection_with_config(&config)
            .await?;
        conn.psubscribe("tasks:*:available").await?;
        Ok(SharedRedisStorage { conn, registry })
    }
}

impl<Args> MakeShared<Args, RedisStorage<Args, MultiplexedConnection>> for SharedRedisStorage {

    type Config = RedisConfig;

    type MakeError = RedisError;

    fn make_shared(&mut self) -> Result<RedisStorage<Args, MultiplexedConnection>, Self::MakeError>
    where
        Self::Config: Default,
    {
        let config = RedisConfig::default().set_namespace(std::any::type_name::<Args>());
        Self::make_shared_with_config(self, config)
    }

    fn make_shared_with_config(
        &mut self,
        config: Self::Config,
    ) -> Result<RedisStorage<Args, MultiplexedConnection>, Self::MakeError> {
        let poller = Arc::new(Event::new());
        self.registry
            .lock()
            .unwrap()
            .insert(config.namespace.clone(), poller.clone());
        let conn = self.conn.clone();
        Ok(RedisStorage {
            conn,
            job_type: PhantomData,
            config,
            codec: PhantomData,
            poller,
        })
    }
}

pub struct RedisSink<Args, Codec, Conn = ConnectionManager> {
    _args: PhantomData<(Args, Codec)>,
    config: RedisConfig,
    pending: Vec<Request<Vec<u8>, RedisContext>>,
    conn: Conn,
    invoke_future: Option<BoxFuture<'static, Result<u32, RedisError>>>,
}

#[derive(Clone)]
pub struct RedisAck<Conn = ConnectionManager> {
    conn: Conn,
    config: RedisConfig,
}

impl<Conn: ConnectionLike + Send + Clone + 'static, Res: Serialize> Acknowledge<Res, RedisContext>
    for RedisAck<Conn>
{
    type Future = BoxFuture<'static, Result<(), Self::Error>>;

    type Error = RedisError;

    fn ack(&mut self, res: &Result<Res, BoxDynError>, parts: &Parts<RedisContext>) -> Self::Future {
        let task_id = parts.task_id.to_string();
        let attempt = parts.attempt.current();
        let worker_id = &parts.context.lock_by.as_ref().unwrap();
        let inflight_set = format!("{}:{}", self.config.inflight_jobs_set(), worker_id);
        let done_jobs_set = self.config.done_jobs_set();
        let dead_jobs_set = self.config.dead_jobs_set();
        let job_meta_hash = self.config.job_meta_hash();
        let status = if res.is_ok() { "ok" } else { "err" };
        let res = res.as_ref().map_err(|e| e.to_string());
        let result_data = JsonCodec::<Vec<u8>>::encode(&res)
            .map_err(|e| build_error("could not encode result"))
            .unwrap();
        let timestamp = Utc::now().timestamp();
        let script = Script::new(include_str!("../lua/ack_job.lua"));
        let mut conn = self.conn.clone();

        async move {
            let mut script = script.key(inflight_set);
            let _ = script
                .key(done_jobs_set)
                .key(dead_jobs_set)
                .key(job_meta_hash)
                .arg(task_id)
                .arg(timestamp)
                .arg(result_data)
                .arg(status)
                .arg(attempt)
                .invoke_async::<u32>(&mut conn)
                .boxed()
                .await?;
            Ok(())
        }
        .boxed()
    }
}

impl<T: Serialize + Send + Unpin, Cdc: Send + Unpin, Conn: ConnectionLike + Send + Unpin>
    TaskSink<T> for RedisSink<T, Cdc, Conn>
{
    type Error = RedisError;

    type Codec = Cdc;

    type Compact = Vec<u8>;

    type Context = RedisContext;

    type Timestamp = u64;
    async fn push_raw_request(
        &mut self,
        request: Request<Self::Compact, Self::Context>,
    ) -> Result<Parts<Self::Context>, Self::Error> {
        let task_id = request.parts.task_id.to_string();

        let parts: Parts<RedisContext> = request.parts;

        let push_job = redis::Script::new(include_str!("../lua/push_job.lua"));

        let _ = push_job
            .key(&self.config.job_data_hash())
            .key(&self.config.active_jobs_list())
            .key(&self.config.signal_list())
            .key(&self.config.job_meta_hash())
            .arg(&task_id)
            .arg(&request.args)
            .arg(&parts.context.max_attempts) // max_attempts
            .invoke_async::<u32>(&mut self.conn)
            .await?;
        Ok(parts)
    }
}

static BATCH_PUSH_SCRIPT: LazyLock<Script> =
    LazyLock::new(|| Script::new(include_str!("../lua/batch_push.lua")));
async fn push_tasks<Conn: ConnectionLike>(
    tasks: Vec<Request<Vec<u8>, RedisContext>>,
    config: RedisConfig,
    mut conn: Conn,
) -> Result<u32, RedisError> {
    let mut batch = BATCH_PUSH_SCRIPT.key(config.job_data_hash());
    let mut script = batch
        .key(config.active_jobs_list())
        .key(config.signal_list())
        .key(config.job_meta_hash());
    for request in tasks {
        let task_id = request.parts.task_id.to_string();
        let attempts = request.parts.attempt.current() as u32;
        let max_attempts = request.parts.context.max_attempts;
        let job = request.args;
        script = script.arg(task_id).arg(job).arg(attempts).arg(max_attempts);
    }

    script.invoke_async::<u32>(&mut conn).await
}

impl<Args, Cdc, Conn> Sink<Request<Args, RedisContext>> for RedisSink<Args, Cdc, Conn>
where
    Args: Unpin + Serialize,
    Cdc: Unpin + Encoder<Args, Compact = Vec<u8>>,
    Conn: ConnectionLike + Unpin + Send + Clone + 'static,
{
    type Error = RedisError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // TODO: can we handle back pressure here?
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        item: Request<Args, RedisContext>,
    ) -> Result<(), Self::Error> {
        let this = Pin::get_mut(self);
        let req = item
            .try_map(|req| Cdc::encode(&req))
            .map_err(|_| RedisError::from((ErrorKind::IoError, "Encoding error")))?;
        this.pending.push(req);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = Pin::get_mut(self);

        // If there's no in-flight Redis future and we have pending items, build the future
        if this.invoke_future.is_none() && !this.pending.is_empty() {
            let tasks: Vec<_> = this.pending.drain(..).collect();
            let fut = push_tasks(tasks, this.config.clone(), this.conn.clone());

            this.invoke_future = Some(fut.boxed());
        }

        // If we have a future in flight, poll it
        if let Some(fut) = &mut this.invoke_future {
            match fut.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    // ✅ Clear the future after it completes
                    this.invoke_future = None;

                    // Propagate the Redis result
                    Poll::Ready(result.map(|_| ()))
                }
            }
        } else {
            // No pending work, flush is complete
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::<Request<Args, RedisContext>>::poll_flush(self, cx)
    }
}

impl RedisConfig {
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

    /// Returns the Redis key for the hash storing job metadata associated with the queue.
    /// The key is dynamically generated using the namespace of the queue.
    ///
    /// # Returns
    /// A `String` representing the Redis key for the job meta hash.
    pub fn job_meta_hash(&self) -> String {
        JOB_META_HASH.replace("{queue}", &self.namespace)
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

#[cfg(test)]
mod tests {
    use std::{fmt::Debug, ops::Deref, sync::atomic::AtomicUsize, time::Duration};

    use futures::{future::ready, SinkExt, TryFutureExt};
    use redis::{parse_redis_url, Client, ConnectionInfo, IntoConnectionInfo};

    use apalis_core::{
        backend::{memory::MemoryStorage, TaskSink},
        request::data::Data,
        service_fn::{self, service_fn, ServiceFn},
        worker::{
            builder::WorkerBuilder,
            ext::{
                ack::AcknowledgementExt, circuit_breaker::CircuitBreaker,
                event_listener::EventListenerExt, long_running::LongRunningExt,
            },
        },
        workflow::{
            stepped::{
                assert_stepped, ExecutionContext, StartStepSinkExt, StepBuilder, StepRequest,
            },
            GoTo,
        },
    };
    use tokio::task::JoinError;

    use super::*;

    const ITEMS: u32 = 10000;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn basic_worker() {
        let client = Client::open("redis://127.0.0.1:6666/").unwrap();
        let conn = client.get_connection_manager().await.unwrap();
        let backend = RedisStorage::new_with_config(
            conn,
            RedisConfig::default().set_namespace("redis_basic_worker").set_buffer_size(100),
        );
        let mut sink = backend.sink();
        for i in 0..ITEMS {
            let mut req: Request<u32, RedisContext> = Request::new(i);
            req.parts.context.max_attempts = i;
            sink.send(req).await.unwrap();
        }

        async fn task(task: u32, ctx: RedisContext, wrk: WorkerContext) -> Result<(), BoxDynError> {
            let handle = std::thread::current();
            // println!("{task:?}, {ctx:?}, Thread: {:?}", handle.id());
            if task == ITEMS - 1 {
                wrk.stop().unwrap();
                return Err("Worker stopped!")?;
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                // println!("CTX {:?}, On Event = {:?}", ctx.get_service(), ev);
            })
            .chain(|s| {
                s.map_future(|f| async {
                    let fut = tokio::spawn(f);
                    let fut = fut.await?;
                    fut
                })
            })
            .build(task);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn shared_workers() {
        let client = Client::open("redis://127.0.0.1/?protocol=resp3").unwrap();
        let mut store = SharedRedisStorage::new(client).await.unwrap();

        let string_store = store
            .make_shared_with_config(
                RedisConfig::default()
                    .set_namespace("strrrrrr")
                    .set_poll_interval(Duration::from_secs(1))
                    .set_buffer_size(5),
            )
            .unwrap();
        let int_store = store
            .make_shared_with_config(
                RedisConfig::default()
                    .set_namespace("Intttttt")
                    .set_poll_interval(Duration::from_secs(2))
                    .set_buffer_size(5),
            )
            .unwrap();
        let mut int_sink = int_store.sink();
        let mut string_sink = string_store.sink();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            for i in 0..ITEMS {
                string_sink.push(format!("ITEM: {i}")).await.unwrap();
                int_sink.push(i).await.unwrap();
            }
        });

        async fn task(job: u32, ctx: WorkerContext) -> Result<usize, BoxDynError> {
            tokio::time::sleep(Duration::from_millis(2)).await;
            if job == ITEMS - 1 {
                ctx.stop().unwrap();
                return Err("Worker stopped!")?;
            }
            Ok(job as usize)
        }

        let int_worker = WorkerBuilder::new("rango-tango-int")
            .backend(int_store)
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
            })
            .build(task)
            .run();

        let string_worker = WorkerBuilder::new("rango-tango-string")
            .backend(string_store)
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
            })
            .build(|req: String, ctx: WorkerContext| async move {
                tokio::time::sleep(Duration::from_millis(3)).await;
                println!("{req}");
                if req.ends_with(&(ITEMS - 1).to_string()) {
                    ctx.stop().unwrap();
                }
            })
            .run();
        let _ = futures::future::join(int_worker, string_worker).await;
    }

    #[tokio::test]
    async fn stepped_workflow() {
        async fn task1(job: u32) -> Result<GoTo<()>, BoxDynError> {
            println!("{job}");
            Ok(GoTo::Next(()))
        }

        async fn task2(_: ()) -> Result<GoTo<usize>, BoxDynError> {
            Ok(GoTo::Next(1))
        }

        async fn task3(
            job: usize,
            wrk: WorkerContext,
            ctx: Data<ExecutionContext>,
        ) -> Result<GoTo<()>, io::Error> {
            wrk.stop().unwrap();
            println!("{job}");
            dbg!(&ctx);
            Ok(GoTo::Done(()))
        }

        async fn recover<Req: Debug>(req: Req) -> Result<(), BoxDynError> {
            println!("Recovering request: {req:?}");
            Err("Unable to recover".into())
        }

        let steps = StepBuilder::new()
            .step_fn(task1)
            .step_fn(task2)
            .step_fn(task3)
            .fallback(recover);

        // assert_stepped::<RedisStorage<StepRequest<Vec<u8>>>, _, _, _, _, _, _, _>(&steps);

        let client = Client::open("redis://127.0.0.1/").unwrap();
        let conn = client.get_connection_manager().await.unwrap();
        let backend = RedisStorage::new_with_config(
            conn,
            RedisConfig::default().set_namespace("redis_workflow"),
        );
        let mut sink = backend.sink();
        let _res = sink.push_start(0u32).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                use apalis_core::worker::event::Event;
                println!("Worker {:?}, On Event = {:?}", ctx.name(), ev);
                if matches!(ev, Event::Error(_)) {
                    ctx.stop().unwrap();
                }
            })
            .build(steps);
        let mut event_stream = worker.stream();
        while let Some(Ok(ev)) = event_stream.next().await {
            println!("On Event = {:?}", ev);
        }
    }
}
