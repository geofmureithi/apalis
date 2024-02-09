use apalis_core::codec::json::JsonCodec;
use apalis_core::data::Extensions;
use apalis_core::error::Error;
use apalis_core::layers::{Ack, AckLayer};
use apalis_core::poller::controller::Controller;
use apalis_core::poller::stream::BackendStream;
use apalis_core::poller::Poller;
use apalis_core::request::{Request, RequestStream};
use apalis_core::storage::{Job, Storage};
use apalis_core::task::attempt::Attempt;
use apalis_core::task::task_id::TaskId;
use apalis_core::worker::WorkerId;
use apalis_core::{Backend, Codec};
use async_stream::try_stream;
use futures::{FutureExt, TryStreamExt};
use log::*;
use redis::ErrorKind;
use redis::{aio::ConnectionManager, Client, IntoConnectionInfo, RedisError, Script, Value};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use std::{
    marker::PhantomData,
    time::{Duration, SystemTime},
};

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

#[derive(Clone, Debug)]
struct RedisQueueInfo {
    active_jobs_list: String,
    consumers_set: String,
    dead_jobs_set: String,
    done_jobs_set: String,
    failed_jobs_set: String,
    inflight_jobs_set: String,
    job_data_hash: String,
    scheduled_jobs_set: String,
    signal_list: String,
}

#[derive(Clone, Debug)]
struct RedisScript {
    ack_job: Script,
    enqueue_scheduled: Script,
    get_jobs: Script,
    kill_job: Script,
    push_job: Script,
    reenqueue_active: Script,
    reenqueue_orphaned: Script,
    register_consumer: Script,
    retry_job: Script,
    schedule_job: Script,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RedisJob<J> {
    ctx: Context,
    job: J,
}

impl<T> From<RedisJob<T>> for Request<T> {
    fn from(val: RedisJob<T>) -> Self {
        let mut data = Extensions::new();
        data.insert(val.ctx.id.clone());
        data.insert(Attempt::new_with_value(val.ctx.attempts));
        data.insert(val.ctx);
        Request::new_with_data(val.job, data)
    }
}

impl<T> From<Request<T>> for RedisJob<T> {
    fn from(val: Request<T>) -> Self {
        let task_id = val.get::<TaskId>().cloned().unwrap();
        let attempts = val.get::<Attempt>().cloned().unwrap_or_default();
        RedisJob {
            job: val.take(),
            ctx: Context {
                attempts: attempts.current(),
                id: task_id,
            },
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Context {
    id: TaskId,
    attempts: usize,
}

/// Config for a [RedisStorage]
#[derive(Clone, Debug)]
pub struct Config {
    fetch_interval: Duration,
    buffer_size: usize,
    max_retries: usize,
    keep_alive: Duration,
    enqueue_scheduled: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            fetch_interval: Duration::from_millis(100),
            buffer_size: 10,
            max_retries: 5,
            keep_alive: Duration::from_secs(30),
            enqueue_scheduled: Duration::from_secs(30),
        }
    }
}

impl Config {
    /// Get the rate of polling per unit of time
    pub fn get_fetch_interval(&self) -> &Duration {
        &self.fetch_interval
    }

    /// Get the number of jobs to fetch
    pub fn get_buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Get the max retries
    pub fn get_max_retries(&self) -> usize {
        self.max_retries
    }

    /// get the keep live rate
    pub fn get_keep_alive(&self) -> &Duration {
        &self.keep_alive
    }

    /// get the enqueued setting
    pub fn get_enqueue_scheduled(&self) -> &Duration {
        &self.enqueue_scheduled
    }

    /// get the fetch interval
    pub fn set_fetch_interval(&mut self, fetch_interval: Duration) {
        self.fetch_interval = fetch_interval;
    }

    /// set the buffer setting
    pub fn set_buffer_size(&mut self, buffer_size: usize) {
        self.buffer_size = buffer_size;
    }

    /// set the max-retries setting
    pub fn set_max_retries(&mut self, max_retries: usize) {
        self.max_retries = max_retries;
    }

    /// set the keep-alive setting
    pub fn set_keep_alive(&mut self, keep_alive: Duration) {
        self.keep_alive = keep_alive;
    }

    /// get the enqueued setting
    pub fn set_enqueue_scheduled(&mut self, enqueue_scheduled: Duration) {
        self.enqueue_scheduled = enqueue_scheduled;
    }
}

type InnerCodec<T> = Arc<
    Box<dyn Codec<RedisJob<T>, Vec<u8>, Error = apalis_core::error::Error> + Sync + Send + 'static>,
>;

/// Represents a [Storage] that uses Redis for storage.
pub struct RedisStorage<T> {
    conn: ConnectionManager,
    job_type: PhantomData<T>,
    queue: RedisQueueInfo,
    scripts: RedisScript,
    controller: Controller,
    config: Config,
    codec: InnerCodec<T>,
}

impl<T> fmt::Debug for RedisStorage<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisStorage")
            .field("conn", &"ConnectionManager")
            .field("job_type", &std::any::type_name::<T>())
            .field("queue", &self.queue)
            .field("scripts", &self.scripts)
            .field("config", &self.config)
            .finish()
    }
}

impl<T> Clone for RedisStorage<T> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            job_type: PhantomData,
            queue: self.queue.clone(),
            scripts: self.scripts.clone(),
            controller: self.controller.clone(),
            config: self.config.clone(),
            codec: self.codec.clone(),
        }
    }
}

impl<T: Job + Serialize + DeserializeOwned> RedisStorage<T> {
    /// Start a new connection
    pub fn new(conn: ConnectionManager) -> Self {
        let name = T::NAME;
        RedisStorage {
            conn,
            job_type: PhantomData,
            controller: Controller::new(),
            config: Config::default(),
            codec: Arc::new(Box::new(JsonCodec)),
            queue: RedisQueueInfo {
                active_jobs_list: ACTIVE_JOBS_LIST.replace("{queue}", name),
                consumers_set: CONSUMERS_SET.replace("{queue}", name),
                dead_jobs_set: DEAD_JOBS_SET.replace("{queue}", name),
                done_jobs_set: DONE_JOBS_SET.replace("{queue}", name),
                failed_jobs_set: FAILED_JOBS_SET.replace("{queue}", name),
                inflight_jobs_set: INFLIGHT_JOB_SET.replace("{queue}", name),
                job_data_hash: JOB_DATA_HASH.replace("{queue}", name),
                scheduled_jobs_set: SCHEDULED_JOBS_SET.replace("{queue}", name),
                signal_list: SIGNAL_LIST.replace("{queue}", name),
            },
            scripts: RedisScript {
                ack_job: redis::Script::new(include_str!("../lua/ack_job.lua")),
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
            },
        }
    }

    /// Get current connection
    pub fn get_connection(&self) -> ConnectionManager {
        self.conn.clone()
    }
}

impl<T: Job + Serialize + DeserializeOwned + Sync + Send + Unpin + 'static> Backend<Request<T>>
    for RedisStorage<T>
{
    type Stream = BackendStream<RequestStream<Request<T>>>;

    type Layer = AckLayer<RedisStorage<T>, T>;

    fn common_layer(&self, worker_id: WorkerId) -> Self::Layer {
        AckLayer::new(self.clone(), worker_id)
    }

    fn poll(self, worker: WorkerId) -> Poller<Self::Stream> {
        let mut storage = self.clone();
        let controller = self.controller.clone();
        let config = self.config.clone();
        let stream: RequestStream<Request<T>> = Box::pin(
            self.stream_jobs(&worker, config.fetch_interval, config.buffer_size)
                .map_err(|e| Error::SourceError(e.into())),
        );

        let keep_alive = async move {
            loop {
                storage.keep_alive::<Self::Layer>(&worker).await.unwrap();
                apalis_core::sleep(config.keep_alive).await;
            }
        }
        .boxed();
        let mut storage = self.clone();
        let enqueue_scheduled = async move {
            loop {
                storage.enqueue_scheduled(config.buffer_size).await.unwrap();
                apalis_core::sleep(config.enqueue_scheduled).await;
            }
        }
        .boxed();
        let heartbeat = async move {
            futures::join!(enqueue_scheduled, keep_alive);
        };
        Poller::new(BackendStream::new(stream, controller), heartbeat.boxed())
    }
}

impl<T: Sync> Ack<T> for RedisStorage<T> {
    type Acknowledger = TaskId;
    type Error = RedisError;
    async fn ack(
        &self,
        worker_id: &WorkerId,
        task_id: &Self::Acknowledger,
    ) -> Result<(), RedisError> {
        let mut conn = self.conn.clone();
        let ack_job = self.scripts.ack_job.clone();
        let inflight_set = format!("{}:{}", self.queue.inflight_jobs_set, worker_id);
        let done_jobs_set = &self.queue.done_jobs_set.to_string();

        let now: i64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .try_into()
            .unwrap();

        ack_job
            .key(inflight_set)
            .key(done_jobs_set)
            .arg(task_id.to_string())
            .arg(now)
            .invoke_async(&mut conn)
            .await
    }
}

impl<T: DeserializeOwned + Send + Unpin + Send + Sync + 'static> RedisStorage<T> {
    fn stream_jobs(
        &self,
        worker_id: &WorkerId,
        interval: Duration,
        buffer_size: usize,
    ) -> RequestStream<Request<T>> {
        let mut conn = self.conn.clone();
        let fetch_jobs = self.scripts.get_jobs.clone();
        let consumers_set = self.queue.consumers_set.to_string();
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let inflight_set = format!("{}:{}", self.queue.inflight_jobs_set, worker_id);
        let signal_list = self.queue.signal_list.to_string();
        let codec = self.codec.clone();
        Box::pin(try_stream! {
            loop {
                apalis_core::sleep(interval).await;
                let result = fetch_jobs
                    .key(&consumers_set)
                    .key(&active_jobs_list)
                    .key(&inflight_set)
                    .key(&job_data_hash)
                    .key(&signal_list)
                    .arg(buffer_size) // No of jobs to fetch
                    .arg(&inflight_set)
                    .invoke_async::<_, Vec<Value>>(&mut conn).await;
                match result {
                    Ok(jobs) => {
                        for job in jobs {
                            yield deserialize_job(&job).map(|res| codec.decode(res)).map(|res| res.unwrap().into())
                        }
                    },
                    Err(e) => {
                        warn!("An error occurred during streaming jobs: {e}");
                    }
                }


            }
        })
    }
}

fn deserialize_job(job: &Value) -> Option<&Vec<u8>> {
    let job = match job {
        job @ Value::Data(_) => Some(job),
        Value::Bulk(val) => val.first(),
        _ => {
            error!(
                "Decoding Message Failed: {:?}",
                "unknown result type for next message"
            );
            None
        }
    };

    match job {
        Some(Value::Data(v)) => Some(v),
        None => None,
        _ => {
            error!("Decoding Message Failed: {:?}", "Expected Data(&Vec<u8>)");
            None
        }
    }
}

impl<T> RedisStorage<T> {
    async fn keep_alive<S>(&mut self, worker_id: &WorkerId) -> Result<(), RedisError> {
        let mut conn = self.conn.clone();
        let register_consumer = self.scripts.register_consumer.clone();
        let inflight_set = format!("{}:{}", self.queue.inflight_jobs_set, worker_id);
        let consumers_set = self.queue.consumers_set.to_string();

        let now: i64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .try_into()
            .unwrap();

        register_consumer
            .key(consumers_set)
            .arg(now)
            .arg(inflight_set)
            .invoke_async(&mut conn)
            .await
    }
}

impl<T> Storage for RedisStorage<T>
where
    T: Serialize + DeserializeOwned + Send + 'static + Unpin + Job + Sync,
{
    type Job = T;
    type Error = RedisError;
    type Identifier = TaskId;

    async fn push(&mut self, job: Self::Job) -> Result<TaskId, RedisError> {
        let mut conn = self.conn.clone();
        let push_job = self.scripts.push_job.clone();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let signal_list = self.queue.signal_list.to_string();
        let job_id = TaskId::new();
        let ctx = Context {
            attempts: 0,
            id: job_id.clone(),
        };
        let job = self.codec.encode(&RedisJob { ctx, job }).unwrap();
        push_job
            .key(job_data_hash)
            .key(active_jobs_list)
            .key(signal_list)
            .arg(job_id.to_string())
            .arg(job)
            .invoke_async(&mut conn)
            .await?;
        Ok(job_id.clone())
    }

    async fn schedule(&mut self, job: Self::Job, on: i64) -> Result<TaskId, RedisError> {
        let mut conn = self.conn.clone();
        let schedule_job = self.scripts.schedule_job.clone();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let scheduled_jobs_set = self.queue.scheduled_jobs_set.to_string();
        let job_id = TaskId::new();
        let ctx = Context {
            attempts: 0,
            id: job_id.clone(),
        };
        let job = RedisJob { job, ctx };
        let job = self.codec.encode(&job).unwrap();
        schedule_job
            .key(job_data_hash)
            .key(scheduled_jobs_set)
            .arg(job_id.to_string())
            .arg(job)
            .arg(on)
            .invoke_async(&mut conn)
            .await?;
        Ok(job_id.clone())
    }

    async fn len(&self) -> Result<i64, RedisError> {
        let mut conn = self.conn.clone();
        let all_jobs: i64 = redis::cmd("HLEN")
            .arg(&self.queue.job_data_hash.to_string())
            .query_async(&mut conn)
            .await?;
        let done_jobs: i64 = redis::cmd("ZCOUNT")
            .arg(self.queue.done_jobs_set.to_owned())
            .arg("-inf")
            .arg("+inf")
            .query_async(&mut conn)
            .await?;
        Ok(all_jobs - done_jobs)
    }

    async fn fetch_by_id(&self, job_id: &TaskId) -> Result<Option<Request<Self::Job>>, RedisError> {
        let mut conn = self.conn.clone();
        let data: Value = redis::cmd("HMGET")
            .arg(&self.queue.job_data_hash.to_string())
            .arg(job_id.to_string())
            .query_async(&mut conn)
            .await?;
        let job = deserialize_job(&data);
        match job {
            None => Err(RedisError::from((
                ErrorKind::ResponseError,
                "Invalid data returned by storage",
            ))),
            Some(bytes) => {
                let inner = self.codec.decode(bytes).unwrap();
                Ok(Some(inner.into()))
            }
        }
    }
    async fn update(&self, job: Request<T>) -> Result<(), RedisError> {
        let job = job.into();
        let mut conn = self.conn.clone();
        let bytes = self.codec.encode(&job).unwrap();
        let _: i64 = redis::cmd("HSET")
            .arg(&self.queue.job_data_hash.to_string())
            .arg(job.ctx.id.to_string())
            .arg(bytes)
            .query_async(&mut conn)
            .await?;
        Ok(())
    }

    async fn reschedule(&mut self, job: Request<T>, wait: Duration) -> Result<(), RedisError> {
        let mut conn = self.conn.clone();
        let schedule_job = self.scripts.schedule_job.clone();
        let job_id = job.get::<TaskId>().cloned().unwrap();
        let worker_id = job.get::<WorkerId>().cloned().unwrap();
        let job = self.codec.encode(&(job.into())).unwrap();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let scheduled_jobs_set = self.queue.scheduled_jobs_set.to_string();
        let on: i64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .try_into()
            .unwrap();
        let wait: i64 = wait.as_secs().try_into().unwrap();
        let inflight_set = format!("{}:{}", self.queue.inflight_jobs_set, worker_id);
        let failed_jobs_set = self.queue.failed_jobs_set.to_string();
        redis::cmd("SREM")
            .arg(inflight_set)
            .arg(job_id.to_string())
            .query_async(&mut conn)
            .await?;
        redis::cmd("ZADD")
            .arg(failed_jobs_set)
            .arg(on)
            .arg(job_id.to_string())
            .query_async(&mut conn)
            .await?;
        schedule_job
            .key(job_data_hash)
            .key(scheduled_jobs_set)
            .arg(job_id.to_string())
            .arg(job)
            .arg(on + wait)
            .invoke_async(&mut conn)
            .await
    }
    async fn is_empty(&self) -> Result<bool, RedisError> {
        todo!()
    }
}

impl<T> RedisStorage<T> {
    /// Attempt to retry a job
    pub async fn retry(&mut self, worker_id: &WorkerId, task_id: &TaskId) -> Result<i32, RedisError>
    where
        T: Send + DeserializeOwned + Serialize + Job + Unpin + Sync + 'static,
    {
        let mut conn = self.conn.clone();
        let retry_job = self.scripts.retry_job.clone();
        let inflight_set = format!("{}:{}", self.queue.inflight_jobs_set, worker_id);
        let scheduled_jobs_set = self.queue.scheduled_jobs_set.to_string();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let job_fut = self.fetch_by_id(task_id);
        let failed_jobs_set = self.queue.failed_jobs_set.to_string();
        let mut storage = self.clone();
        let now: i64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .try_into()
            .unwrap();
        let res = job_fut.await?;
        match res {
            Some(job) => {
                let attempt = job.get::<Attempt>().cloned().unwrap_or_default();
                if attempt.current() >= self.config.max_retries {
                    redis::cmd("ZADD")
                        .arg(failed_jobs_set)
                        .arg(now)
                        .arg(task_id.to_string())
                        .query_async(&mut conn)
                        .await?;
                    storage.kill(worker_id, task_id).await?;
                    return Ok(1);
                }
                let job = self.codec.encode(&(job.into())).unwrap();

                let res: Result<i32, RedisError> = retry_job
                    .key(inflight_set)
                    .key(scheduled_jobs_set)
                    .key(job_data_hash)
                    .arg(task_id.to_string())
                    .arg(now)
                    .arg(job)
                    .invoke_async(&mut conn)
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
    pub async fn kill(&mut self, worker_id: &WorkerId, task_id: &TaskId) -> Result<(), RedisError>
    where
        T: Send + DeserializeOwned + Serialize + Job + Unpin + Sync + 'static,
    {
        let mut conn = self.conn.clone();
        let kill_job = self.scripts.kill_job.clone();
        let current_worker_id = format!("{}:{}", self.queue.inflight_jobs_set, worker_id);
        let job_data_hash = self.queue.job_data_hash.to_string();
        let dead_jobs_set = self.queue.dead_jobs_set.to_string();
        let fetch_job = self.fetch_by_id(task_id);
        let now: i64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .try_into()
            .unwrap();
        let res = fetch_job.await?;
        match res {
            Some(job) => {
                let data = self.codec.encode(&job.into()).unwrap();
                kill_job
                    .key(current_worker_id)
                    .key(dead_jobs_set)
                    .key(job_data_hash)
                    .arg(task_id.to_string())
                    .arg(now)
                    .arg(data)
                    .invoke_async(&mut conn)
                    .await
            }
            None => Err(RedisError::from((ErrorKind::ResponseError, "Id not found"))),
        }
    }

    /// Required to add scheduled jobs to the active set
    pub async fn enqueue_scheduled(&mut self, count: usize) -> Result<usize, RedisError> {
        let enqueue_jobs = self.scripts.enqueue_scheduled.clone();
        let scheduled_jobs_set = self.queue.scheduled_jobs_set.to_string();
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let signal_list = self.queue.signal_list.to_string();
        let now: i64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .try_into()
            .unwrap();
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
        let mut conn = self.conn.clone();
        let reenqueue_active = self.scripts.reenqueue_active.clone();
        let inflight_set = self.queue.inflight_jobs_set.to_string();
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let signal_list = self.queue.signal_list.to_string();

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
            .invoke_async(&mut conn)
            .await
    }
    /// Re-enqueue some jobs that might be orphaned.
    pub async fn reenqueue_orphaned(
        &mut self,
        count: usize,
        dead_since: i64,
    ) -> Result<usize, RedisError> {
        let reenqueue_orphaned = self.scripts.reenqueue_orphaned.clone();
        let consumers_set = self.queue.consumers_set.to_string();
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let signal_list = self.queue.signal_list.to_string();

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
    use email_service::Email;
    use futures::StreamExt;

    use super::*;

    /// migrate DB and return a storage instance.
    async fn setup() -> RedisStorage<Email> {
        let redis_url = std::env::var("REDIS_URL").expect("No REDIS_URL is specified");
        // Because connections cannot be shared across async runtime
        // (different runtimes are created for each test),
        // we don't share the storage and tests must be run sequentially.
        let conn = connect(redis_url).await.unwrap();
        let storage = RedisStorage::new(conn);
        storage
    }

    /// rollback DB changes made by tests.
    ///
    /// You should execute this function in the end of a test
    async fn cleanup(mut storage: RedisStorage<Email>, _worker_id: &WorkerId) {
        let _resp: String = redis::cmd("FLUSHDB")
            .query_async(&mut storage.conn)
            .await
            .expect("failed to Flushdb");
    }

    struct DummyService {}

    fn example_email() -> Email {
        Email {
            subject: "Test Subject".to_string(),
            to: "example@postgres".to_string(),
            text: "Some Text".to_string(),
        }
    }

    async fn consume_one(storage: &RedisStorage<Email>, worker_id: &WorkerId) -> Request<Email> {
        let mut stream = storage.stream_jobs(worker_id, std::time::Duration::from_secs(10), 1);
        stream
            .next()
            .await
            .expect("stream is empty")
            .expect("failed to poll job")
            .expect("no job is pending")
    }

    async fn register_worker_at(storage: &mut RedisStorage<Email>) -> WorkerId {
        let worker = WorkerId::new("test-worker");

        storage
            .keep_alive::<DummyService>(&worker)
            .await
            .expect("failed to register worker");
        worker
    }

    async fn register_worker(storage: &mut RedisStorage<Email>) -> WorkerId {
        register_worker_at(storage).await
    }

    async fn push_email(storage: &mut RedisStorage<Email>, email: Email) {
        storage.push(email).await.expect("failed to push a job");
    }

    async fn get_job(storage: &mut RedisStorage<Email>, job_id: &TaskId) -> Request<Email> {
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

        let _job = consume_one(&mut storage, &worker_id).await;

        cleanup(storage, &worker_id).await;
    }

    #[tokio::test]
    async fn test_acknowledge_job() {
        let mut storage = setup().await;
        push_email(&mut storage, example_email()).await;

        let worker_id = register_worker(&mut storage).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let job_id = &job.get::<Context>().unwrap().id;

        storage
            .ack(&worker_id, &job_id)
            .await
            .expect("failed to acknowledge the job");

        let _job = get_job(&mut storage, &job_id).await;
        cleanup(storage, &worker_id).await;
    }

    #[tokio::test]
    async fn test_kill_job() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let worker_id = register_worker(&mut storage).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let job_id = &job.get::<Context>().unwrap().id;

        storage
            .kill(&worker_id, &job_id)
            .await
            .expect("failed to kill job");

        let _job = get_job(&mut storage, &job_id).await;

        cleanup(storage, &worker_id).await;
    }

    #[tokio::test]
    async fn test_heartbeat_renqueueorphaned_pulse_last_seen_6min() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let worker_id = register_worker_at(&mut storage).await;

        let _job = consume_one(&mut storage, &worker_id).await;
        storage
            .reenqueue_orphaned(5, 300)
            .await
            .expect("failed to reenqueue_orphaned");
        cleanup(storage, &worker_id).await;
    }

    #[tokio::test]
    async fn test_heartbeat_renqueueorphaned_pulse_last_seen_4min() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let worker_id = register_worker_at(&mut storage).await;

        let _job = consume_one(&mut storage, &worker_id).await;
        let result = storage
            .reenqueue_orphaned(5, 300)
            .await
            .expect("failed to reenqueue_orphaned");

        cleanup(storage, &worker_id).await;
    }
}
