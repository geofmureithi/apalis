use std::{
    marker::PhantomData,
    time::{Duration, Instant},
};

use apalis_core::{
    error::JobStreamError,
    job::{Job, JobId, JobStreamResult},
    request::JobRequest,
    storage::{Storage, StorageError, StorageResult, StorageWorkerPulse},
    utils::Timer,
    worker::WorkerId,
};
use async_stream::try_stream;
use futures::{Stream, TryStreamExt};
use log::*;
use redis::{aio::ConnectionManager, Client, IntoConnectionInfo, RedisError, Script, Value};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt;

use crate::Timestamp;

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

/// Represents a [Storage] that uses Redis for storage.
pub struct RedisStorage<T> {
    conn: ConnectionManager,
    job_type: PhantomData<T>,
    queue: RedisQueueInfo,
    scripts: RedisScript,
}

impl<T> fmt::Debug for RedisStorage<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisStorage")
            .field("conn", &"ConnectionManager")
            .field("job_type", &std::any::type_name::<T>())
            .field("queue", &self.queue)
            .field("scripts", &self.scripts)
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
        }
    }
}

impl<T: Job> RedisStorage<T> {
    /// Start a new connection
    pub fn new(conn: ConnectionManager) -> Self {
        let name = T::NAME;
        RedisStorage {
            conn,
            job_type: PhantomData,
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

    /// Connect to a redis url
    pub async fn connect<S: IntoConnectionInfo>(redis: S) -> Result<Self, RedisError> {
        let client = Client::open(redis.into_connection_info()?)?;
        let conn = client.get_tokio_connection_manager().await?;
        Ok(Self::new(conn))
    }

    /// Get current connection
    pub fn get_connection(&self) -> ConnectionManager {
        self.conn.clone()
    }
}

impl<T: DeserializeOwned + Send + Unpin> RedisStorage<T> {
    fn stream_jobs(
        &self,
        worker_id: &WorkerId,
        interval: Duration,
        buffer_size: usize,
    ) -> impl Stream<Item = Result<Option<JobRequest<T>>, JobStreamError>> {
        let mut conn = self.conn.clone();
        let fetch_jobs = self.scripts.get_jobs.clone();
        let consumers_set = self.queue.consumers_set.to_string();
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let inflight_set = format!("{}:{}", self.queue.inflight_jobs_set, worker_id);
        let signal_list = self.queue.signal_list.to_string();
        #[cfg(feature = "async-std-comp")]
        #[allow(unused_variables)]
        let sleeper = apalis_core::utils::timer::AsyncStdTimer;
        #[cfg(feature = "tokio-comp")]
        let sleeper = apalis_core::utils::timer::TokioTimer;
        try_stream! {
            loop {
                sleeper.sleep_until(Instant::now() + interval).await;
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
                            yield deserialize_job::<JobRequest<T>>(&job)
                        }
                    },
                    Err(e) => {
                        warn!("An error occurred during streaming jobs: {e}");
                    }
                }


            }
        }
    }
}

fn deserialize_job<T>(job: &Value) -> Option<T>
where
    T: DeserializeOwned,
{
    let job = match job {
        job @ Value::Data(_) => Some(job),
        Value::Bulk(val) => val.get(0),
        _ => {
            error!(
                "Decoding Message Failed: {:?}",
                "unknown result type for next message"
            );
            None
        }
    };

    match job {
        Some(Value::Data(v)) => serde_json::from_slice(v).ok(),
        None => None,
        _ => {
            error!("Decoding Message Failed: {:?}", "Expected Data(&Vec<u8>)");
            None
        }
    }
}

#[async_trait::async_trait]
impl<T> Storage for RedisStorage<T>
where
    T: Serialize + DeserializeOwned + Send + 'static + Unpin + Job + Sync,
{
    type Output = T;

    async fn push(&mut self, job: Self::Output) -> StorageResult<JobId> {
        let mut conn = self.conn.clone();
        let push_job = self.scripts.push_job.clone();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let signal_list = self.queue.signal_list.to_string();
        let job = JobRequest::new(job);
        let job_id = job.id();
        let job = serde_json::to_string(&job).map_err(|e| StorageError::Parse(e.into()))?;
        log::debug!(
            "Received new job with id: {} to list: {}",
            job_id,
            active_jobs_list
        );

        push_job
            .key(job_data_hash)
            .key(active_jobs_list)
            .key(signal_list)
            .arg(job_id.to_string())
            .arg(job)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(job_id.clone())
    }

    async fn schedule(&mut self, job: Self::Output, on: Timestamp) -> StorageResult<JobId> {
        let mut conn = self.conn.clone();
        let schedule_job = self.scripts.schedule_job.clone();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let scheduled_jobs_set = self.queue.scheduled_jobs_set.to_string();
        let job = JobRequest::new(job);
        let job_id = job.id();
        let job = serde_json::to_string(&job).map_err(|e| StorageError::Parse(e.into()))?;
        log::trace!(
            "Scheduled new job with id: {} to list: {}",
            job_id,
            scheduled_jobs_set
        );

        #[cfg(feature = "chrono")]
        let timestamp = on.timestamp();
        #[cfg(all(not(feature = "chrono"), feature = "time"))]
        let timestamp = on.unix_timestamp();

        schedule_job
            .key(job_data_hash)
            .key(scheduled_jobs_set)
            .arg(job_id.to_string())
            .arg(job)
            .arg(timestamp)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        Ok(job_id.clone())
    }

    fn consume(
        &mut self,
        worker_id: &WorkerId,
        interval: Duration,
        buffer_size: usize,
    ) -> JobStreamResult<T> {
        Box::pin(
            self.stream_jobs(worker_id, interval, buffer_size)
                .map_ok(|job| match job {
                    Some(mut job) => {
                        let id = job.id().clone();
                        job.insert(id);
                        Some(job)
                    }
                    None => None,
                }),
        )
    }

    async fn kill(&mut self, worker_id: &WorkerId, job_id: &JobId) -> StorageResult<()> {
        let mut conn = self.conn.clone();
        let kill_job = self.scripts.kill_job.clone();
        let current_worker_id = format!("{}:{}", self.queue.inflight_jobs_set, worker_id);
        let job_data_hash = self.queue.job_data_hash.to_string();
        let dead_jobs_set = self.queue.dead_jobs_set.to_string();
        let fetch_job = self.fetch_by_id(job_id);

        let res = fetch_job.await?;
        match res {
            Some(job) => {
                #[cfg(feature = "chrono")]
                let now = chrono::Utc::now().timestamp();
                #[cfg(feature = "time")]
                let now = time::OffsetDateTime::now_utc().unix_timestamp();

                let data =
                    serde_json::to_string(&job).map_err(|e| StorageError::Parse(e.into()))?;
                kill_job
                    .key(current_worker_id)
                    .key(dead_jobs_set)
                    .key(job_data_hash)
                    .arg(job_id.to_string())
                    .arg(now)
                    .arg(data)
                    .invoke_async(&mut conn)
                    .await
                    .map_err(|e| StorageError::Database(Box::from(e)))
            }
            None => Err(StorageError::NotFound),
        }
    }

    async fn len(&self) -> StorageResult<i64> {
        let mut conn = self.conn.clone();
        let all_jobs: i64 = redis::cmd("HLEN")
            .arg(&self.queue.job_data_hash.to_string())
            .query_async(&mut conn)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        let done_jobs: i64 = redis::cmd("ZCOUNT")
            .arg(self.queue.done_jobs_set.to_owned())
            .arg("-inf")
            .arg("+inf")
            .query_async(&mut conn)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(all_jobs - done_jobs)
    }

    async fn fetch_by_id(&self, job_id: &JobId) -> StorageResult<Option<JobRequest<Self::Output>>> {
        let mut conn = self.conn.clone();
        let data: Value = redis::cmd("HMGET")
            .arg(&self.queue.job_data_hash.to_string())
            .arg(job_id.to_string())
            .query_async(&mut conn)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(deserialize_job(&data))
    }
    async fn update_by_id(&self, job_id: &JobId, job: &JobRequest<T>) -> StorageResult<()> {
        let mut conn = self.conn.clone();
        let job = serde_json::to_string(job).map_err(|e| StorageError::Parse(e.into()))?;
        let res: Result<i64, RedisError> = redis::cmd("HSET")
            .arg(&self.queue.job_data_hash.to_string())
            .arg(job_id.to_string())
            .arg(job)
            .query_async(&mut conn)
            .await;
        let _res = res.map_err(|e| StorageError::Database(Box::new(e)))?;
        Ok(())
    }

    async fn ack(&mut self, worker_id: &WorkerId, job_id: &JobId) -> StorageResult<()> {
        let mut conn = self.conn.clone();
        let ack_job = self.scripts.ack_job.clone();
        let inflight_set = format!("{}:{}", self.queue.inflight_jobs_set, worker_id);
        let done_jobs_set = &self.queue.done_jobs_set.to_string();

        #[cfg(feature = "chrono")]
        let now = chrono::Utc::now().timestamp();
        #[cfg(feature = "time")]
        let now = time::OffsetDateTime::now_utc().unix_timestamp();
        ack_job
            .key(inflight_set)
            .key(done_jobs_set)
            .arg(job_id.to_string())
            .arg(now)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| StorageError::Database(Box::new(e)))
    }
    async fn retry(&mut self, worker_id: &WorkerId, job_id: &JobId) -> StorageResult<()> {
        let mut conn = self.conn.clone();
        let retry_job = self.scripts.retry_job.clone();
        let inflight_set = format!("{}:{}", self.queue.inflight_jobs_set, worker_id);
        let scheduled_jobs_set = self.queue.scheduled_jobs_set.to_string();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let job_fut = self.fetch_by_id(job_id);
        let failed_jobs_set = self.queue.failed_jobs_set.to_string();
        let mut storage = self.clone();

        let res = job_fut.await?;

        #[cfg(feature = "chrono")]
        let now = chrono::Utc::now().timestamp();
        #[cfg(feature = "time")]
        let now = time::OffsetDateTime::now_utc().unix_timestamp();

        match res {
            Some(job) => {
                if job.attempts() >= job.max_attempts() {
                    warn!("too many retries: {:?}", job.attempts());
                    let _res = redis::cmd("ZADD")
                        .arg(failed_jobs_set)
                        .arg(now)
                        .arg(job_id.to_string())
                        .query_async(&mut conn)
                        .await
                        .map_err(|e| StorageError::Database(Box::new(e)))?;
                    storage
                        .kill(worker_id, job_id)
                        .await
                        .map_err(|e| StorageError::Database(Box::new(e)))?;
                    return Ok(());
                }
                let job = serde_json::to_string(&job).map_err(|e| StorageError::Parse(e.into()))?;
                #[cfg(feature = "chrono")]
                let now = chrono::Utc::now().timestamp();
                #[cfg(feature = "time")]
                let now = time::OffsetDateTime::now_utc().unix_timestamp();
                let res: Result<i32, StorageError> = retry_job
                    .key(inflight_set)
                    .key(scheduled_jobs_set)
                    .key(job_data_hash)
                    .arg(job_id.to_string())
                    .arg(now)
                    .arg(job)
                    .invoke_async(&mut conn)
                    .await
                    .map_err(|e| StorageError::Database(Box::new(e)));
                match res {
                    Ok(count) => {
                        trace!("Jobs to remove: {:?}", count);
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            }
            None => Err(StorageError::NotFound),
        }
    }
    async fn keep_alive<Service>(&mut self, worker_id: &WorkerId) -> StorageResult<()> {
        let mut conn = self.conn.clone();
        let register_consumer = self.scripts.register_consumer.clone();
        let inflight_set = format!("{}:{}", self.queue.inflight_jobs_set, worker_id);
        let consumers_set = self.queue.consumers_set.to_string();
        #[cfg(feature = "chrono")]
        let timestamp = chrono::Utc::now().timestamp();
        #[cfg(feature = "time")]
        let timestamp = time::OffsetDateTime::now_utc().unix_timestamp();

        register_consumer
            .key(consumers_set)
            .arg(timestamp)
            .arg(inflight_set)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| StorageError::Connection(Box::from(e)))
    }

    async fn heartbeat(&mut self, beat: StorageWorkerPulse) -> StorageResult<bool> {
        let mut conn = self.conn.clone();
        match beat {
            StorageWorkerPulse::EnqueueScheduled { count } => {
                let enqueue_jobs = self.scripts.enqueue_scheduled.clone();
                let scheduled_jobs_set = self.queue.scheduled_jobs_set.to_string();
                let active_jobs_list = self.queue.active_jobs_list.to_string();
                let signal_list = self.queue.signal_list.to_string();
                #[cfg(feature = "chrono")]
                let timestamp = chrono::Utc::now().timestamp();
                #[cfg(feature = "time")]
                let timestamp = time::OffsetDateTime::now_utc().unix_timestamp();
                let res: Result<i8, StorageError> = enqueue_jobs
                    .key(scheduled_jobs_set)
                    .key(active_jobs_list)
                    .key(signal_list)
                    .arg(timestamp)
                    .arg(count)
                    .invoke_async(&mut conn)
                    .await
                    .map_err(|e| StorageError::Database(Box::from(e)));
                match res {
                    Ok(count) => {
                        if count > 0 {
                            trace!("Jobs to enqueue: {:?}", count);
                        }
                        Ok(true)
                    }
                    Err(e) => Err(e),
                }
            }

            StorageWorkerPulse::ReenqueueOrphaned {
                count,
                timeout_worker,
            } => {
                let reenqueue_orphaned = self.scripts.reenqueue_orphaned.clone();
                let consumers_set = self.queue.consumers_set.to_string();
                let active_jobs_list = self.queue.active_jobs_list.to_string();
                let signal_list = self.queue.signal_list.to_string();
                #[cfg(feature = "chrono")]
                let timestamp = (chrono::Utc::now()
                    - chrono::Duration::seconds(timeout_worker.as_secs() as i64))
                .timestamp();
                #[cfg(feature = "time")]
                let timestamp = (time::OffsetDateTime::now_utc() - timeout_worker).unix_timestamp();
                let res: Result<i8, StorageError> = reenqueue_orphaned
                    .key(consumers_set)
                    .key(active_jobs_list)
                    .key(signal_list)
                    .arg(timestamp)
                    .arg(count)
                    .invoke_async(&mut conn)
                    .await
                    .map_err(|e| StorageError::Database(Box::from(e)));
                match res {
                    Ok(count) => {
                        if count > 0 {
                            debug!("{} jobs to reenqueue", count);
                        }
                        Ok(true)
                    }
                    Err(e) => Err(e),
                }
            }
            _ => todo!(),
        }
    }
    async fn reenqueue_active(&mut self, job_ids: Vec<&JobId>) -> StorageResult<()> {
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
            .map_err(|e| StorageError::Connection(Box::from(e)))
    }
    async fn reschedule(&mut self, job: &JobRequest<T>, _wait: Duration) -> StorageResult<()> {
        let mut conn = self.conn.clone();
        let schedule_job = self.scripts.schedule_job.clone();
        let job_id = job.id();
        let worker_id = job.lock_by().as_ref().ok_or(StorageError::NotFound)?;
        let job = serde_json::to_string(job).map_err(|e| StorageError::Parse(e.into()))?;
        let job_data_hash = self.queue.job_data_hash.to_string();
        let scheduled_jobs_set = self.queue.scheduled_jobs_set.to_string();
        #[cfg(feature = "chrono")]
        let on = chrono::Utc::now().timestamp();
        #[cfg(feature = "time")]
        let on = time::OffsetDateTime::now_utc().unix_timestamp();
        let inflight_set = format!("{}:{}", self.queue.inflight_jobs_set, worker_id);
        let failed_jobs_set = self.queue.failed_jobs_set.to_string();
        let _cmd = redis::cmd("SREM")
            .arg(inflight_set)
            .arg(job_id.to_string())
            .query_async(&mut conn)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        let _cmd = redis::cmd("ZADD")
            .arg(failed_jobs_set)
            .arg(on)
            .arg(job_id.to_string())
            .query_async(&mut conn)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))?;
        schedule_job
            .key(job_data_hash)
            .key(scheduled_jobs_set)
            .arg(job_id.to_string())
            .arg(job)
            .arg(on)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| StorageError::Database(Box::from(e)))
    }
}

#[cfg(feature = "expose")]
#[cfg_attr(docsrs, doc(cfg(feature = "expose")))]
/// Expose an [`SqliteStorage`] for web and cli management tools
pub mod expose {
    use apalis_core::error::JobError;
    use apalis_core::expose::{ExposedWorker, JobStreamExt};
    use apalis_core::request::JobState;
    use apalis_core::storage::StorageError;

    use super::*;

    fn deserialize_multiple_jobs<T>(job: Option<&Value>) -> Option<Vec<T>>
    where
        T: DeserializeOwned,
    {
        let job = match job {
            None => None,
            Some(Value::Bulk(val)) => Some(val),
            _ => {
                error!(
                    "Decoding Message Failed: {:?}",
                    "unknown result type for next message"
                );
                None
            }
        };

        job.map(|values| {
            values
                .iter()
                .filter_map(|v| match v {
                    Value::Data(data) => serde_json::from_slice(data).ok(),
                    _ => None,
                })
                .collect()
        })
    }

    #[async_trait::async_trait]
    impl<T> JobStreamExt<T> for RedisStorage<T>
    where
        T: 'static + Job + Serialize + DeserializeOwned + Send + Unpin + Sync,
    {
        async fn list_jobs(
            &mut self,
            status: &JobState,
            page: i32,
        ) -> Result<Vec<JobRequest<T>>, JobError> {
            match status {
                JobState::Pending => {
                    let mut conn = self.conn.clone();
                    let active_jobs_list = &self.queue.active_jobs_list;
                    let job_data_hash = &self.queue.job_data_hash;
                    let ids: Vec<String> = redis::cmd("LRANGE")
                        .arg(active_jobs_list)
                        .arg(((page - 1) * 10).to_string())
                        .arg((page * 10).to_string())
                        .query_async(&mut conn)
                        .await
                        .map_err(|e| StorageError::Database(Box::new(e)))?;
                    if ids.is_empty() {
                        return Ok(Vec::new());
                    }
                    let data: Option<Value> = redis::cmd("HMGET")
                        .arg(job_data_hash)
                        .arg(&ids)
                        .query_async(&mut conn)
                        .await
                        .map_err(|e| StorageError::Database(Box::new(e)))?;
                    let jobs: Vec<JobRequest<T>> =
                        deserialize_multiple_jobs(data.as_ref()).unwrap();
                    Ok(jobs)
                }
                JobState::Running => {
                    let mut conn = self.conn.clone();
                    let consumers_set = &self.queue.consumers_set;
                    let job_data_hash = &self.queue.job_data_hash;
                    let workers: Vec<String> = redis::cmd("ZRANGE")
                        .arg(consumers_set)
                        .arg("0")
                        .arg("-1")
                        .query_async(&mut conn)
                        .await
                        .map_err(|e| StorageError::Database(Box::new(e)))?;
                    if workers.is_empty() {
                        return Ok(Vec::new());
                    }
                    let mut all_jobs = Vec::new();
                    for worker in workers {
                        let ids: Vec<String> = redis::cmd("SMEMBERS")
                            .arg(&worker)
                            .query_async(&mut conn)
                            .await
                            .map_err(|e| StorageError::Database(Box::new(e)))?;
                        if ids.is_empty() {
                            continue;
                        };
                        let data: Option<Value> = redis::cmd("HMGET")
                            .arg(job_data_hash.clone())
                            .arg(&ids)
                            .query_async(&mut conn)
                            .await
                            .map_err(|e| StorageError::Database(Box::new(e)))?;
                        let jobs: Vec<JobRequest<T>> =
                            deserialize_multiple_jobs(data.as_ref()).unwrap();
                        all_jobs.extend(jobs);
                    }

                    Ok(all_jobs)
                }
                JobState::Done => {
                    let mut conn = self.conn.clone();
                    let done_jobs_set = &self.queue.done_jobs_set;
                    let job_data_hash = &self.queue.job_data_hash;
                    let ids: Vec<String> = redis::cmd("ZRANGE")
                        .arg(done_jobs_set)
                        .arg(((page - 1) * 10).to_string())
                        .arg((page * 10).to_string())
                        .query_async(&mut conn)
                        .await
                        .map_err(|e| StorageError::Database(Box::new(e)))?;
                    if ids.is_empty() {
                        return Ok(Vec::new());
                    }
                    let data: Option<Value> = redis::cmd("HMGET")
                        .arg(job_data_hash)
                        .arg(&ids)
                        .query_async(&mut conn)
                        .await
                        .map_err(|e| StorageError::Database(Box::new(e)))?;
                    let jobs: Vec<JobRequest<T>> =
                        deserialize_multiple_jobs(data.as_ref()).unwrap();
                    Ok(jobs)
                }
                JobState::Retry => Ok(Vec::new()),
                JobState::Failed => {
                    let mut conn = self.conn.clone();
                    let failed_jobs_set = &self.queue.failed_jobs_set;
                    let job_data_hash = &self.queue.job_data_hash;
                    let ids: Vec<String> = redis::cmd("ZRANGE")
                        .arg(failed_jobs_set)
                        .arg(((page - 1) * 10).to_string())
                        .arg((page * 10).to_string())
                        .query_async(&mut conn)
                        .await
                        .map_err(|e| StorageError::Database(Box::new(e)))?;
                    if ids.is_empty() {
                        return Ok(Vec::new());
                    }
                    let data: Option<Value> = redis::cmd("HMGET")
                        .arg(job_data_hash)
                        .arg(&ids)
                        .query_async(&mut conn)
                        .await
                        .map_err(|e| StorageError::Database(Box::new(e)))?;
                    let jobs: Vec<JobRequest<T>> =
                        deserialize_multiple_jobs(data.as_ref()).unwrap();
                    Ok(jobs)
                }
                JobState::Killed => {
                    let mut conn = self.conn.clone();
                    let dead_jobs_set = &self.queue.dead_jobs_set;
                    let job_data_hash = &self.queue.job_data_hash;
                    let ids: Vec<String> = redis::cmd("ZRANGE")
                        .arg(dead_jobs_set)
                        .arg(((page - 1) * 10).to_string())
                        .arg((page * 10).to_string())
                        .query_async(&mut conn)
                        .await
                        .map_err(|e| StorageError::Database(Box::new(e)))?;
                    if ids.is_empty() {
                        return Ok(Vec::new());
                    }
                    let data: Option<Value> = redis::cmd("HMGET")
                        .arg(job_data_hash)
                        .arg(&ids)
                        .query_async(&mut conn)
                        .await
                        .map_err(|e| StorageError::Database(Box::new(e)))?;
                    let jobs: Vec<JobRequest<T>> =
                        deserialize_multiple_jobs(data.as_ref()).unwrap();
                    Ok(jobs)
                }
            }
        }
        async fn list_workers(&mut self) -> Result<Vec<ExposedWorker>, JobError> {
            let consumers_set = &self.queue.consumers_set;

            let mut conn = self.conn.clone();
            let workers: Vec<String> = redis::cmd("ZRANGE")
                .arg(consumers_set)
                .arg("0")
                .arg("-1")
                .query_async(&mut conn)
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;
            Ok(workers
                .into_iter()
                .map(|w| {
                    #[cfg(feature = "chrono")]
                    let now = chrono::Utc::now();
                    #[cfg(all(not(feature = "chrono"), feature = "time"))]
                    let now = time::OffsetDateTime::now_utc();
                    ExposedWorker::new::<Self, T>(
                        WorkerId::new(
                            w.replace(&format!("{}:", &self.queue.inflight_jobs_set), ""),
                        ),
                        "".to_string(),
                        now,
                    )
                })
                .collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apalis_core::context::HasJobContext;
    use apalis_core::request::JobState;
    use email_service::Email;
    use futures::StreamExt;

    /// migrate DB and return a storage instance.
    async fn setup() -> RedisStorage<Email> {
        let redis_url = &std::env::var("REDIS_URL").expect("No REDIS_URL is specified");
        // Because connections cannot be shared across async runtime
        // (different runtimes are created for each test),
        // we don't share the storage and tests must be run sequentially.
        let storage = RedisStorage::connect(redis_url.as_str())
            .await
            .expect("failed to connect DB server");
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

    async fn consume_one<S, T>(storage: &mut S, worker_id: &WorkerId) -> JobRequest<T>
    where
        S: Storage<Output = T>,
    {
        let mut stream = storage.consume(worker_id, std::time::Duration::from_secs(10), 1);
        stream
            .next()
            .await
            .expect("stream is empty")
            .expect("failed to poll job")
            .expect("no job is pending")
    }

    async fn register_worker_at(
        storage: &mut RedisStorage<Email>,
        _last_seen: Timestamp,
    ) -> WorkerId {
        let worker = WorkerId::new("test-worker");

        storage
            .keep_alive::<DummyService>(&worker)
            .await
            .expect("failed to register worker");
        worker
    }

    async fn register_worker(storage: &mut RedisStorage<Email>) -> WorkerId {
        #[cfg(feature = "chrono")]
        let now = chrono::Utc::now();
        #[cfg(feature = "time")]
        let now = time::OffsetDateTime::now_utc();
        register_worker_at(storage, now).await
    }

    async fn push_email<S>(storage: &mut S, email: Email)
    where
        S: Storage<Output = Email>,
    {
        storage.push(email).await.expect("failed to push a job");
    }

    async fn get_job<S>(storage: &mut S, job_id: &JobId) -> JobRequest<Email>
    where
        S: Storage<Output = Email>,
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

        // No worker yet
        // Redis doesn't update jobs like in sql
        assert_eq!(*job.context().status(), JobState::Pending);
        assert_eq!(*job.context().lock_by(), None);
        assert!(job.context().lock_at().is_none());

        cleanup(storage, &worker_id).await;
    }

    #[tokio::test]
    async fn test_acknowledge_job() {
        let mut storage = setup().await;
        push_email(&mut storage, example_email()).await;

        let worker_id = register_worker(&mut storage).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let job_id = job.context().id();

        storage
            .ack(&worker_id, job_id)
            .await
            .expect("failed to acknowledge the job");

        let job = get_job(&mut storage, job_id).await;
        assert_eq!(*job.context().status(), JobState::Pending); // Redis storage uses hset etc to manage status
        assert!(job.context().done_at().is_none());

        cleanup(storage, &worker_id).await;
    }

    #[tokio::test]
    async fn test_kill_job() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let worker_id = register_worker(&mut storage).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let job_id = job.context().id();

        storage
            .kill(&worker_id, job_id)
            .await
            .expect("failed to kill job");

        let job = get_job(&mut storage, job_id).await;
        assert_eq!(*job.context().status(), JobState::Pending);
        assert!(job.context().done_at().is_none());

        cleanup(storage, &worker_id).await;
    }

    #[tokio::test]
    async fn test_heartbeat_renqueueorphaned_pulse_last_seen_6min() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        #[cfg(feature = "chrono")]
        let six_minutes_ago = chrono::Utc::now() - chrono::Duration::minutes(6);
        #[cfg(feature = "time")]
        let six_minutes_ago = time::OffsetDateTime::now_utc() - time::Duration::minutes(6);

        let worker_id = register_worker_at(&mut storage, six_minutes_ago).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let result = storage
            .heartbeat(StorageWorkerPulse::ReenqueueOrphaned {
                count: 5,
                timeout_worker: Duration::from_secs(300),
            })
            .await
            .expect("failed to heartbeat");
        assert!(result);

        let job_id = job.context().id();
        let job = get_job(&mut storage, job_id).await;

        assert_eq!(*job.context().status(), JobState::Pending);
        assert!(job.context().done_at().is_none());
        assert!(job.context().lock_by().is_none());
        assert!(job.context().lock_at().is_none());
        assert_eq!(*job.context().last_error(), None);

        cleanup(storage, &worker_id).await;
    }

    #[tokio::test]
    async fn test_heartbeat_renqueueorphaned_pulse_last_seen_4min() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        #[cfg(feature = "chrono")]
        let four_minutes_ago = chrono::Utc::now() - chrono::Duration::minutes(4);
        #[cfg(feature = "time")]
        let four_minutes_ago = time::OffsetDateTime::now_utc() - time::Duration::minutes(4);

        let worker_id = register_worker_at(&mut storage, four_minutes_ago).await;

        let job = consume_one(&mut storage, &worker_id).await;
        let result = storage
            .heartbeat(StorageWorkerPulse::ReenqueueOrphaned {
                count: 5,
                timeout_worker: Duration::from_secs(300),
            })
            .await
            .expect("failed to heartbeat");
        assert!(result);

        let job_id = job.context().id();
        let job = get_job(&mut storage, job_id).await;

        assert_eq!(*job.context().status(), JobState::Pending);
        assert_eq!(*job.context().lock_by(), None);

        cleanup(storage, &worker_id).await;
    }
}
