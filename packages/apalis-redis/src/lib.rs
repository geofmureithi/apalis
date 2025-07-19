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
    convert::Infallible,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, SystemTime},
};

use apalis_core::{
    backend::{
        codec::{json::JsonCodec, Codec, Encoder},
        shared::{MakeShared, Shared},
        Backend, RequestStream, TaskSink,
    },
    error::BoxDynError,
    request::{Parts, Request},
    service_fn::from_request::FromRequest,
    worker::{
        context::WorkerContext,
        ext::ack::{Acknowledge, AcknowledgeLayer},
    },
};
use futures::{
    future::BoxFuture,
    stream::{self, BoxStream},
    FutureExt, Sink, StreamExt, TryFuture,
};
use redis::{aio::ConnectionLike, aio::MultiplexedConnection, ErrorKind};
// mod expose;
// mod storage;
pub use redis::{aio::ConnectionManager, RedisError};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
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
const SCHEDULED_JOBS_SET: &str = "{queue}:scheduled";
const SIGNAL_LIST: &str = "{queue}:signal";

pub type SharedRedisStorage = RedisStorage<SharedTask, MultiplexedConnection>;

/// The context for a redis storage job
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RedisContext {
    max_attempts: usize,
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
pub struct RedisStorage<Args, Conn = ConnectionManager, C = JsonCodec<Vec<u8>>> {
    conn: Conn,
    job_type: PhantomData<Args>,
    // pub(super) scripts: RedisScript,
    config: RedisConfig,
    codec: PhantomData<C>,
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

impl<Args, Conn: Clone, C> Backend<Args, RedisContext> for RedisStorage<Args, Conn, C> {
    type Stream = RequestStream<Request<Args, RedisContext>>;

    type Error = BoxDynError;
    type Layer = AcknowledgeLayer<RedisAck<Conn>, ()>;
    type Beat = BoxStream<'static, Result<(), Self::Error>>;
    type Sink = RedisSink<Args, C, Conn>;

    fn heartbeat(&self) -> Self::Beat {
        stream::once(async { Ok(()) }).boxed()
    }
    fn middleware(&self) -> Self::Layer {
        AcknowledgeLayer::new(RedisAck {
            conn: self.conn.clone(),
        })
    }

    fn sink(&self) -> Self::Sink {
        RedisSink {
            _args: PhantomData,
            pending: Vec::new(),
            config: self.config.clone(),
            conn: self.conn.clone(),
        }
    }

    fn poll(self, _worker: &WorkerContext) -> Self::Stream {
        todo!()
        // stream
    }
}

pub struct SharedTask(());

impl<Args> MakeShared<Args> for RedisStorage<SharedTask, MultiplexedConnection> {
    type Backend = RedisStorage<Args, MultiplexedConnection>;

    type Config = RedisConfig;

    type MakeError = String;

    fn make_shared_with_config(
        &mut self,
        config: Self::Config,
    ) -> Result<Self::Backend, Self::MakeError> {
        let conn = self.conn.clone();
        Ok(RedisStorage {
            conn,
            job_type: PhantomData,
            config,
            codec: PhantomData,
        })
    }
}

pub struct RedisSink<Args, Codec, Conn = ConnectionManager> {
    _args: PhantomData<(Args, Codec)>,
    config: RedisConfig,
    pending: Vec<Request<Vec<u8>, RedisContext>>,
    conn: Conn,
}

#[derive(Clone)]
pub struct RedisAck<Conn = ConnectionManager> {
    conn: Conn,
}

impl<Conn, Res, Ctx> Acknowledge<Res, Ctx> for RedisAck<Conn> {
    type Future = BoxFuture<'static, Result<(), Self::Error>>;

    type Error = RedisError;

    fn ack(&mut self, res: &Result<Res, BoxDynError>, parts: &Parts<Ctx>) -> Self::Future {
        // TODO
        async { Ok(()) }.boxed()
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
        let task_id = request.parts.task_id.clone();

        let parts: Parts<RedisContext> = request.parts;
        let job: Vec<u8> = request.args;

        let push_job = redis::Script::new(include_str!("../lua/push_job.lua"));

        push_job
            .key(&self.config.get_namespace())
            .key(&self.config.active_jobs_list())
            .key(&self.config.signal_list())
            .arg(task_id.to_string())
            .arg(job)
            .invoke_async::<()>(&mut self.conn)
            .await?;
        Ok(parts)
    }
}

impl<
        Args: Unpin + Serialize,
        Cdc: Unpin + Encoder<Args, Compact = Vec<u8>>,
        Conn: ConnectionLike + Unpin,
    > Sink<Request<Args, RedisContext>> for RedisSink<Args, Cdc, Conn>
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

        let tasks: Vec<_> = this.pending.drain(..).collect();

        let batch_push = redis::Script::new(include_str!("../lua/batch_push.lua"));

        let mut binding = batch_push.key(this.config.get_namespace());
        let mut script = binding
            .key(this.config.active_jobs_list())
            .key(this.config.signal_list());

        for request in tasks {
            let task_id = request.parts.task_id.to_string();
            let job = request.args;

            script = script.arg(task_id).arg(&job);
        }

        let req = std::pin::pin!(script.invoke_async::<u32>(&mut this.conn));

        let result = futures::ready!(req.try_poll(cx)).map(|_| ());
        Poll::Ready(result)
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
    use std::{ops::Deref, sync::atomic::AtomicUsize, time::Duration};

    use futures::future::ready;
    use redis::Client;

    use apalis_core::{
        backend::memory::MemoryStorage,
        backend::TaskSink,
        request::data::Data,
        service_fn::{self, service_fn, ServiceFn},
        worker::builder::WorkerBuilder,
        worker::ext::{
            ack::AcknowledgementExt, circuit_breaker::CircuitBreaker,
            event_listener::EventListenerExt, long_running::LongRunningExt,
        },
    };

    use super::*;

    const ITEMS: u32 = 100;

    #[tokio::test]
    async fn it_works() {
        let client = Client::open("redis://127.0.0.1/").unwrap();
        let conn = client.get_multiplexed_async_connection().await.unwrap();
        let mut store = SharedRedisStorage::new(conn);
        let string_store = store.make_shared().unwrap();
        let int_store = store.make_shared().unwrap();
        let mut int_sink = int_store.sink();
        let mut string_sink = string_store.sink();

        for i in 0..ITEMS {
            string_sink.push(format!("ITEM: {i}")).await.unwrap();
            int_sink.push(i).await.unwrap();
        }

        async fn task(job: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_millis(2)).await;
            if job == ITEMS - 1 {
                ctx.stop();
                return Err("Worker stopped!")?;
            }
            Ok(())
        }

        let string_worker = WorkerBuilder::new("rango-tango-string")
            .backend(string_store)
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
            })
            .build(|req: String, ctx: WorkerContext| async move {
                tokio::time::sleep(Duration::from_millis(2)).await;
                println!("{req}");
                if req.ends_with(&(ITEMS - 1).to_string()) {
                    ctx.stop();
                }
            })
            .run();

        let int_worker = WorkerBuilder::new("rango-tango-int")
            .backend(int_store)
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
            })
            .build(task)
            .run();

        let _ = futures::future::join(int_worker, string_worker).await;
    }
}
