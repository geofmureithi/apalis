use std::{marker::PhantomData, time::Duration};

use apalis_core::{
    error::StorageError,
    job::Job,
    request::{JobRequest, JobState},
    storage::{JobStream, Storage, StorageResult},
    worker::WorkerPulse,
};
use async_stream::try_stream;
use chrono::{DateTime, Utc};
use futures::Stream;
use log::*;
use redis::{aio::MultiplexedConnection, Client, IntoConnectionInfo, RedisError, Script, Value};
use serde::{de::DeserializeOwned, Serialize};

const ACTIVE_JOBS_LIST: &str = "{queue}:active";
const CONSUMERS_SET: &str = "{queue}:consumers";
const DEAD_JOBS_SET: &str = "{queue}:deadjobs";
const INFLIGHT_JOB_SET: &str = "{queue}:inflight";
const JOB_DATA_HASH: &str = "{queue}:data";
const SCHEDULED_JOBS_SET: &str = "{queue}:scheduled";
const SIGNAL_LIST: &str = "{queue}:signal";

#[derive(Clone)]
struct RedisQueueInfo {
    name: String,
    active_jobs_list: String,
    consumers_set: String,
    dead_jobs_set: String,
    inflight_jobs_set: String,
    job_data_hash: String,
    scheduled_jobs_set: String,
    signal_list: String,
}

#[derive(Clone)]
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
    conn: MultiplexedConnection,
    job_type: PhantomData<T>,
    queue: RedisQueueInfo,
    scripts: RedisScript,
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
    pub async fn connect<S: IntoConnectionInfo>(redis: S) -> Result<Self, RedisError> {
        let client = Client::open(redis.into_connection_info()?)?;
        let conn = client.get_multiplexed_async_connection().await?;
        let name = T::NAME;
        Ok(RedisStorage {
            conn,
            job_type: PhantomData,
            queue: RedisQueueInfo {
                name: name.to_string(),
                active_jobs_list: ACTIVE_JOBS_LIST.replace("{queue}", &name),
                consumers_set: CONSUMERS_SET.replace("{queue}", &name),
                dead_jobs_set: DEAD_JOBS_SET.replace("{queue}", &name),
                inflight_jobs_set: INFLIGHT_JOB_SET.replace("{queue}", &name),
                job_data_hash: JOB_DATA_HASH.replace("{queue}", &name),
                scheduled_jobs_set: SCHEDULED_JOBS_SET.replace("{queue}", &name),
                signal_list: SIGNAL_LIST.replace("{queue}", &name),
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
        })
    }
}

impl<T: DeserializeOwned + Send + Unpin> RedisStorage<T> {
    fn stream_jobs(
        &self,
        worker_id: String,
        interval: Duration,
    ) -> impl Stream<Item = Result<Option<JobRequest<T>>, StorageError>> {
        let mut conn = self.conn.clone();
        let fetch_jobs = self.scripts.get_jobs.clone();
        let consumers_set = self.queue.consumers_set.to_string();
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let inflight_set = format!("{}:{}", self.queue.inflight_jobs_set, worker_id);
        let signal_list = self.queue.signal_list.to_string();
        let mut interval = tokio::time::interval(interval);
        try_stream! {
            loop {
                interval.tick().await;
                let res: Result<Vec<Value>, RedisError> = fetch_jobs
                    .key(&consumers_set)
                    .key(&active_jobs_list)
                    .key(&inflight_set)
                    .key(&job_data_hash)
                    .key(&signal_list)
                    .arg("1") // fetch one job at a time
                    .arg(&inflight_set)
                    .invoke_async(&mut conn)
                    .await;
                let job = unwrap_job(res)?;
                yield job
            }
        }
    }
}

fn unwrap_job<T>(
    data: Result<Vec<Value>, RedisError>,
) -> Result<Option<JobRequest<T>>, StorageError>
where
    T: DeserializeOwned,
{
    match data {
        Ok(jobs) => {
            let job = jobs.get(0);
            Ok(deserialize_job(job))
        }
        Err(e) => Err(StorageError::Database(Box::new(e))),
    }
}

fn deserialize_job<T>(job: Option<&Value>) -> Option<T>
where
    T: DeserializeOwned,
{
    let job = match job {
        job @ Some(Value::Data(_)) => job,
        None => None,
        Some(Value::Bulk(val)) => val.get(0),
        _ => {
            error!(
                "Decoding Message Failed: {:?}",
                "unknown result type for next message"
            );
            None
        }
    };

    match job {
        Some(Value::Data(v)) => Some(serde_json::from_slice(&v).unwrap()),
        None => None,
        _ => {
            error!("Decoding Message Failed: {:?}", "Expected Data(&Vec<u8>)");
            None
        }
    }
}

impl<T> Storage for RedisStorage<T>
where
    T: Serialize + DeserializeOwned + Send + 'static + Unpin + Job,
{
    type Output = T;

    fn push(&mut self, job: Self::Output) -> StorageResult<()> {
        let mut conn = self.conn.clone();
        let push_job = self.scripts.push_job.clone();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let signal_list = self.queue.signal_list.to_string();
        let job = JobRequest::new(job);
        let job_id = job.id();
        let job = serde_json::to_string(&job).unwrap();
        log::info!(
            "Received new job with id: {} to list: {}",
            job_id,
            active_jobs_list
        );
        let fut = async move {
            push_job
                .key(job_data_hash)
                .key(active_jobs_list)
                .key(signal_list)
                .arg(job_id)
                .arg(job)
                .invoke_async(&mut conn)
                .await
                .map_err(|e| StorageError::Database(Box::from(e)))
        };
        Box::pin(fut)
    }

    fn schedule(&mut self, job: Self::Output, on: DateTime<Utc>) -> StorageResult<()> {
        let mut conn = self.conn.clone();
        let schedule_job = self.scripts.schedule_job.clone();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let scheduled_jobs_set = self.queue.scheduled_jobs_set.to_string();
        let job = JobRequest::new(job);
        let job_id = job.id();
        let job = serde_json::to_string(&job).unwrap();
        log::trace!(
            "Scheduled new job with id: {} to list: {}",
            job_id,
            scheduled_jobs_set
        );
        let fut = async move {
            schedule_job
                .key(job_data_hash)
                .key(scheduled_jobs_set)
                .arg(job_id)
                .arg(job)
                .arg(on.timestamp())
                .invoke_async(&mut conn)
                .await
                .map_err(|e| StorageError::Database(Box::from(e)))
        };
        Box::pin(fut)
    }

    fn consume(&mut self, worker_id: String, interval: Duration) -> JobStream<T> {
        Box::pin(self.stream_jobs(worker_id, interval))
    }

    fn kill(&mut self, worker_id: String, job_id: String) -> StorageResult<()> {
        let mut conn = self.conn.clone();
        let kill_job = self.scripts.kill_job.clone();
        let inflight_set = format!("{}:{}", self.queue.inflight_jobs_set, worker_id);
        let job_data_hash = self.queue.job_data_hash.to_string();
        let dead_jobs_set = self.queue.dead_jobs_set.to_string();
        let fetch_job = self.fetch_by_id(job_id.clone());
        let fut = async move {
            let res = fetch_job.await?;

            match res {
                Some(job) => {
                    let data = serde_json::to_string(&job)?;
                    kill_job
                        .key(inflight_set)
                        .key(dead_jobs_set)
                        .key(job_data_hash)
                        .arg(job_id)
                        .arg(data)
                        .invoke_async(&mut conn)
                        .await
                        .map_err(|e| StorageError::Database(Box::from(e)))
                }
                None => Err(StorageError::NotFound),
            }
        };
        Box::pin(fut)
    }

    fn len(&self) -> StorageResult<i64> {
        let mut conn = self.conn.clone();
        let job_data_hash = format!("{}", &self.queue.job_data_hash);
        let fut = async move {
            let length: i64 = redis::cmd("HLEN")
                .arg(format!("{}", job_data_hash))
                .query_async(&mut conn)
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;
            Ok(length)
        };
        Box::pin(fut)
    }

    fn fetch_by_id(&self, job_id: String) -> StorageResult<Option<JobRequest<Self::Output>>> {
        let mut conn = self.conn.clone();
        let job_data_hash = format!("{}", &self.queue.job_data_hash);
        let fut = async move {
            let data: Option<Value> = redis::cmd("HMGET")
                .arg(format!("{}", job_data_hash))
                .arg(job_id.clone())
                .query_async(&mut conn)
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))?;
            Ok(deserialize_job(data.as_ref()))
        };
        Box::pin(fut)
    }
    fn update_by_id(&self, job_id: String, job: &JobRequest<T>) -> StorageResult<()> {
        let mut conn = self.conn.clone();
        let job_data_hash = format!("{}", &self.queue.job_data_hash);
        let job = serde_json::to_string(job).unwrap();
        let fut = async move {
            let res: Result<i64, RedisError> = redis::cmd("HSET")
                .arg(format!("{}", job_data_hash))
                .arg(job_id)
                .arg(job)
                .query_async(&mut conn)
                .await;
            let _res = res.map_err(|e| StorageError::Database(Box::new(e)))?;
            Ok(())
        };
        Box::pin(fut)
    }

    fn ack(&mut self, worker_id: String, job_id: String) -> StorageResult<()> {
        let mut conn = self.conn.clone();
        let ack_job = self.scripts.ack_job.clone();
        let inflight_set = format!("{}:{}", self.queue.inflight_jobs_set, worker_id);
        let data_hash = format!("{}", &self.queue.job_data_hash);
        let fut = async move {
            ack_job
                .key(inflight_set)
                .key(data_hash)
                .arg(job_id)
                .invoke_async(&mut conn)
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))
        };
        Box::pin(fut)
    }
    fn retry(&mut self, worker_id: String, job_id: String) -> StorageResult<()> {
        let mut conn = self.conn.clone();
        let retry_job = self.scripts.retry_job.clone();
        let inflight_set = format!("{}:{}", self.queue.inflight_jobs_set, worker_id);
        let scheduled_jobs_set = self.queue.scheduled_jobs_set.to_string();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let job_fut = self.fetch_by_id(job_id.clone());
        let fut = async move {
            let res = job_fut.await?;
            match res {
                Some(job) => {
                    if job.attempts() >= job.max_attempts() {
                        warn!("too many retries: {:?}", job.attempts());
                        return Ok(());
                    }
                    let job = serde_json::to_string(&job)?;
                    let now = Utc::now().timestamp();
                    let res: Result<i32, StorageError> = retry_job
                        .key(inflight_set)
                        .key(scheduled_jobs_set)
                        .key(job_data_hash)
                        .arg(job_id)
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
        };
        Box::pin(fut)
    }
    fn keep_alive(&mut self, worker_id: String) -> StorageResult<()> {
        let mut conn = self.conn.clone();
        let register_consumer = self.scripts.register_consumer.clone();
        let inflight_set = format!("{}:{}", self.queue.inflight_jobs_set, worker_id);
        let consumers_set = self.queue.consumers_set.to_string();
        let timestamp = Utc::now().timestamp();
        let fut = async move {
            register_consumer
                .key(consumers_set)
                .arg(timestamp)
                .arg(inflight_set)
                .invoke_async(&mut conn)
                .await
                .map_err(|e| StorageError::Connection(Box::from(e)))
        };
        Box::pin(fut)
    }

    fn heartbeat(&mut self, beat: WorkerPulse) -> StorageResult<bool> {
        let mut conn = self.conn.clone();
        match beat {
            WorkerPulse::EnqueueScheduled { count } => {
                let enqueue_jobs = self.scripts.enqueue_scheduled.clone();
                let scheduled_jobs_set = self.queue.scheduled_jobs_set.to_string();
                let active_jobs_list = self.queue.active_jobs_list.to_string();
                let signal_list = self.queue.signal_list.to_string();
                let timestamp = Utc::now().timestamp();
                let fut = async move {
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
                };
                Box::pin(fut)
            }

            WorkerPulse::RenqueueOrpharned { count } => {
                let reenqueue_orphaned = self.scripts.reenqueue_orphaned.clone();
                let consumers_set = self.queue.consumers_set.to_string();
                let active_jobs_list = self.queue.active_jobs_list.to_string();
                let signal_list = self.queue.signal_list.to_string();
                let timestamp = Utc::now() - chrono::Duration::minutes(5);
                let fut = async move {
                    let res: Result<i8, StorageError> = reenqueue_orphaned
                        .key(consumers_set)
                        .key(active_jobs_list)
                        .key(signal_list)
                        .arg(timestamp.timestamp())
                        .arg(count)
                        .invoke_async(&mut conn)
                        .await
                        .map_err(|e| StorageError::Database(Box::from(e)));
                    match res {
                        Ok(count) => {
                            if count > 0 {
                                info!("{} jobs to reenqueue", count);
                            }
                            Ok(true)
                        }
                        Err(e) => Err(e),
                    }
                };
                Box::pin(fut)
            }
            _ => todo!(),
        }
    }
    fn reenqueue_active(&mut self, job_ids: Vec<String>) -> StorageResult<()> {
        let mut conn = self.conn.clone();
        let reenqueue_active = self.scripts.reenqueue_active.clone();
        let inflight_set = self.queue.inflight_jobs_set.to_string();
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let signal_list = self.queue.signal_list.to_string();
        let fut = async move {
            reenqueue_active
                .key(inflight_set)
                .key(active_jobs_list)
                .key(signal_list)
                .arg(job_ids)
                .invoke_async(&mut conn)
                .await
                .map_err(|e| StorageError::Connection(Box::from(e)))
        };
        Box::pin(fut)
    }
}
