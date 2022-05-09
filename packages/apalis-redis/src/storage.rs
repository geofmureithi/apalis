use std::{marker::PhantomData, time::Duration};

use apalis_core::{
    error::StorageError,
    queue::Heartbeat,
    request::JobRequest,
    storage::{JobStream, Storage, StorageResult},
};
use async_stream::try_stream;
use chrono::Utc;
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
    inflight_jobs_prefix: String,
    job_data_hash: String,
    scheduled_jobs_set: String,
    signal_list: String,
}

#[derive(Clone)]
struct RedisScript {
    ack_job: Script,
    push_job: Script,
    retry_job: Script,
    push_scheduled: Script,
    get_jobs: Script,
    register_storage: Script,
}

/// Represents a [Storage] that uses Redis for storage.
pub struct RedisStorage<T> {
    id: uuid::Uuid,
    conn: MultiplexedConnection,
    job_type: PhantomData<T>,
    queue: RedisQueueInfo,
    scripts: RedisScript,
}

impl<T> Clone for RedisStorage<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            conn: self.conn.clone(),
            job_type: PhantomData,
            queue: self.queue.clone(),
            scripts: self.scripts.clone(),
        }
    }
}

impl<T> RedisStorage<T> {
    pub async fn new<S: IntoConnectionInfo>(redis: S) -> Result<Self, RedisError> {
        let client = Client::open(redis.into_connection_info()?)?;
        let conn = client.get_multiplexed_async_connection().await?;
        let name = std::any::type_name::<T>();
        Ok(RedisStorage {
            id: uuid::Uuid::new_v4(),
            conn,
            job_type: PhantomData,
            queue: RedisQueueInfo {
                name: name.to_string(),
                active_jobs_list: ACTIVE_JOBS_LIST.replace("{queue}", &name),
                consumers_set: CONSUMERS_SET.replace("{queue}", &name),
                dead_jobs_set: DEAD_JOBS_SET.replace("{queue}", &name),
                inflight_jobs_prefix: INFLIGHT_JOB_SET.replace("{queue}", &name),
                job_data_hash: JOB_DATA_HASH.replace("{queue}", &name),
                scheduled_jobs_set: SCHEDULED_JOBS_SET.replace("{queue}", &name),
                signal_list: SIGNAL_LIST.replace("{queue}", &name),
            },
            scripts: RedisScript {
                ack_job: redis::Script::new(include_str!("../lua/ack_job.lua")),
                push_job: redis::Script::new(include_str!("../lua/push_job.lua")),
                retry_job: redis::Script::new(include_str!("../lua/retry_job.lua")),
                push_scheduled: redis::Script::new(include_str!(
                    "../lua/enqueue_scheduled_jobs.lua"
                )),
                get_jobs: redis::Script::new(include_str!("../lua/get_jobs.lua")),
                register_storage: redis::Script::new(include_str!("../lua/register_consumer.lua")),
            },
        })
    }
}

impl<T: DeserializeOwned + Send + Unpin> RedisStorage<T> {
    fn stream_jobs(&self) -> impl Stream<Item = Result<Option<JobRequest<T>>, StorageError>> {
        let mut conn = self.conn.clone();
        let fetch_jobs = self.scripts.get_jobs.clone();
        let consumers_set = self.queue.consumers_set.to_string();
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let inflight_set = format!("{}:{}", self.queue.inflight_jobs_prefix, self.id);
        let signal_list = self.queue.signal_list.to_string();
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        try_stream! {
            loop {
                interval.tick().await;
                let res: Result<Vec<Value>, RedisError> = fetch_jobs
                    .key(&consumers_set)
                    .key(&active_jobs_list)
                    .key(&inflight_set)
                    .key(&job_data_hash)
                    .key(&signal_list)
                    .arg("1") // Fetch Ten Job at a time
                    .arg(&inflight_set)
                    .invoke_async(&mut conn)
                    .await;
                let job = deserialize(res)?;
                yield job
            }
        }
    }
}

fn deserialize<T>(
    data: Result<Vec<Value>, RedisError>,
) -> Result<Option<JobRequest<T>>, StorageError>
where
    T: DeserializeOwned,
{
    match data {
        Ok(jobs) => {
            let job = jobs.get(0);
            let job = match job {
                job @ Some(Value::Data(_)) => job,
                None => None,
                _ => {
                    error!(
                        "Decoding Message Failed: {:?}",
                        "unknown result type for next message"
                    );
                    None
                }
            };

            match &job {
                Some(Value::Data(v)) => Ok(Some(serde_json::from_slice(&v).unwrap())),
                None => Ok(None),
                _ => {
                    error!("Decoding Message Failed: {:?}", "Expected Data(&Vec<u8>)");
                    Ok(None)
                }
            }
        }
        Err(e) => Err(StorageError::Database(Box::new(e))),
    }
}

impl<T> Storage for RedisStorage<T>
where
    T: Serialize + DeserializeOwned + Send + 'static + Unpin,
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
        log::info!("Received new job with id: {}", job_id);
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

    fn consume(&mut self) -> JobStream<T> {
        Box::pin(self.stream_jobs())
    }

    fn ack(&mut self, job_id: String) -> StorageResult<()> {
        let mut conn = self.conn.clone();
        let ack_job = self.scripts.ack_job.clone();
        let inflight_set = format!("{}:{}", &self.queue.inflight_jobs_prefix, &self.id);
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
    fn retry(&mut self, job_id: String) -> StorageResult<()> {
        let conn = self.conn.clone();
        let retry_jobs = self.scripts.retry_job.clone();
        let inflight_set = format!("{}:{}", &self.queue.inflight_jobs_prefix, &self.id);
        let scheduled_jobs_set = self.queue.scheduled_jobs_set.to_string();
        let job_data_hash = self.queue.job_data_hash.to_string();

        let fut = async move {
            unimplemented!("Get the stored data");
            retry_jobs
                .key(inflight_set)
                .key(scheduled_jobs_set)
                .key(job_data_hash)
                .arg(job_id)
                .arg(Utc::now().timestamp())
                .arg(job_id)
                .invoke_async(&mut conn)
                .await
                .map_err(|e| StorageError::Database(Box::new(e)))
        };
        Box::pin(fut)
    }
    fn heartbeat(&mut self, beat: Heartbeat) -> StorageResult<bool> {
        let mut conn = self.conn.clone();
        match beat {
            Heartbeat::EnqueueScheduled(count) => {
                let enqueue_jobs = self.scripts.push_scheduled.clone();
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
                                info!("Jobs to enqueue: {:?}", count);
                            }
                            Ok(true)
                        }
                        Err(e) => Err(e),
                    }
                };
                Box::pin(fut)
            }
            Heartbeat::Register => {
                let mut conn = self.conn.clone();
                let register_consumer = self.scripts.register_storage.clone();
                let inflight_set = format!("{}:{}", &self.queue.inflight_jobs_prefix, self.id);
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
            Heartbeat::RenqueueActive => todo!(),
            Heartbeat::RenqueueOrpharned => todo!(),
            Heartbeat::Other(_) => todo!(),
        }
    }
}
