use crate::RedisContext;
use crate::RedisStorage;
use apalis_core::backend::BackendExpose;
use apalis_core::backend::Stat;
use apalis_core::backend::WorkerState;
use apalis_core::codec::json::JsonCodec;
use apalis_core::codec::Codec;
use apalis_core::request::Request;
use apalis_core::request::State;
use apalis_core::worker::Worker;
use apalis_core::worker::WorkerId;
use redis::{ErrorKind, Value};
use serde::{de::DeserializeOwned, Serialize};

type RedisCodec = JsonCodec<Vec<u8>>;

impl<T> BackendExpose<T> for RedisStorage<T>
where
    T: 'static + Serialize + DeserializeOwned + Send + Unpin + Sync,
{
    type Request = Request<T, RedisContext>;
    type Error = redis::RedisError;
    async fn stats(&self) -> Result<Stat, redis::RedisError> {
        let mut conn = self.get_connection().clone();
        let queue = self.get_config();

        let stats_script = self.scripts.stats.clone();

        let results: Vec<usize> = stats_script
            .key(queue.active_jobs_list())
            .key(queue.consumers_set())
            .key(queue.dead_jobs_set())
            .key(queue.failed_jobs_set())
            .key(queue.done_jobs_set())
            .invoke_async(&mut conn)
            .await?;

        Ok(Stat {
            pending: results[0],
            running: results[1],
            dead: results[2],
            failed: results[3],
            success: results[4],
        })
    }
    async fn list_jobs(
        &self,
        status: &State,
        page: i32,
    ) -> Result<Vec<Self::Request>, redis::RedisError> {
        let mut conn = self.get_connection().clone();
        let queue = self.get_config();
        match status {
            State::Pending | State::Scheduled => {
                let active_jobs_list = &queue.active_jobs_list();
                let job_data_hash = &queue.job_data_hash();
                let ids: Vec<String> = redis::cmd("LRANGE")
                    .arg(active_jobs_list)
                    .arg(((page - 1) * 10).to_string())
                    .arg((page * 10).to_string())
                    .query_async(&mut conn)
                    .await?;

                if ids.is_empty() {
                    return Ok(Vec::new());
                }
                let data: Option<Value> = redis::cmd("HMGET")
                    .arg(job_data_hash)
                    .arg(&ids)
                    .query_async(&mut conn)
                    .await?;

                let jobs: Vec<Request<T, RedisContext>> =
                    deserialize_multiple_jobs::<_, RedisCodec>(data.as_ref()).unwrap();
                Ok(jobs)
            }
            State::Running => {
                let consumers_set = &queue.consumers_set();
                let job_data_hash = &queue.job_data_hash();
                let workers: Vec<String> = redis::cmd("ZRANGE")
                    .arg(consumers_set)
                    .arg("0")
                    .arg("-1")
                    .query_async(&mut conn)
                    .await?;

                if workers.is_empty() {
                    return Ok(Vec::new());
                }
                let mut all_jobs = Vec::new();
                for worker in workers {
                    let ids: Vec<String> = redis::cmd("SMEMBERS")
                        .arg(&worker)
                        .query_async(&mut conn)
                        .await?;

                    if ids.is_empty() {
                        continue;
                    };
                    let data: Option<Value> = redis::cmd("HMGET")
                        .arg(job_data_hash.clone())
                        .arg(&ids)
                        .query_async(&mut conn)
                        .await?;

                    let jobs: Vec<Request<T, RedisContext>> =
                        deserialize_multiple_jobs::<_, RedisCodec>(data.as_ref()).unwrap();
                    all_jobs.extend(jobs);
                }

                Ok(all_jobs)
            }
            State::Done => {
                let done_jobs_set = &queue.done_jobs_set();
                let job_data_hash = &queue.job_data_hash();
                let ids: Vec<String> = redis::cmd("ZRANGE")
                    .arg(done_jobs_set)
                    .arg(((page - 1) * 10).to_string())
                    .arg((page * 10).to_string())
                    .query_async(&mut conn)
                    .await?;

                if ids.is_empty() {
                    return Ok(Vec::new());
                }
                let data: Option<Value> = redis::cmd("HMGET")
                    .arg(job_data_hash)
                    .arg(&ids)
                    .query_async(&mut conn)
                    .await?;

                let jobs: Vec<Request<T, RedisContext>> =
                    deserialize_multiple_jobs::<_, RedisCodec>(data.as_ref()).unwrap();
                Ok(jobs)
            }
            // State::Retry => Ok(Vec::new()),
            State::Failed => {
                let failed_jobs_set = &queue.failed_jobs_set();
                let job_data_hash = &queue.job_data_hash();
                let ids: Vec<String> = redis::cmd("ZRANGE")
                    .arg(failed_jobs_set)
                    .arg(((page - 1) * 10).to_string())
                    .arg((page * 10).to_string())
                    .query_async(&mut conn)
                    .await?;
                if ids.is_empty() {
                    return Ok(Vec::new());
                }
                let data: Option<Value> = redis::cmd("HMGET")
                    .arg(job_data_hash)
                    .arg(&ids)
                    .query_async(&mut conn)
                    .await?;
                let jobs: Vec<Request<T, RedisContext>> =
                    deserialize_multiple_jobs::<_, RedisCodec>(data.as_ref()).unwrap();

                Ok(jobs)
            }
            State::Killed => {
                let dead_jobs_set = &queue.dead_jobs_set();
                let job_data_hash = &queue.job_data_hash();
                let ids: Vec<String> = redis::cmd("ZRANGE")
                    .arg(dead_jobs_set)
                    .arg(((page - 1) * 10).to_string())
                    .arg((page * 10).to_string())
                    .query_async(&mut conn)
                    .await?;

                if ids.is_empty() {
                    return Ok(Vec::new());
                }
                let data: Option<Value> = redis::cmd("HMGET")
                    .arg(job_data_hash)
                    .arg(&ids)
                    .query_async(&mut conn)
                    .await?;

                let jobs: Vec<Request<T, RedisContext>> =
                    deserialize_multiple_jobs::<_, RedisCodec>(data.as_ref()).unwrap();

                Ok(jobs)
            }
        }
    }
    async fn list_workers(&self) -> Result<Vec<Worker<WorkerState>>, redis::RedisError> {
        let queue = self.get_config();
        let consumers_set = &queue.consumers_set();
        let mut conn = self.get_connection().clone();
        let workers: Vec<String> = redis::cmd("ZRANGE")
            .arg(consumers_set)
            .arg("0")
            .arg("-1")
            .query_async(&mut conn)
            .await?;
        Ok(workers
            .into_iter()
            .map(|w| {
                Worker::new(
                    WorkerId::new(w.replace(&format!("{}:", &queue.inflight_jobs_set()), "")),
                    WorkerState::new::<Self>(queue.get_namespace().to_owned()),
                )
            })
            .collect())
    }
}

fn deserialize_multiple_jobs<T, C: Codec<Compact = Vec<u8>>>(
    jobs: Option<&Value>,
) -> Option<Vec<Request<T, RedisContext>>>
where
    T: DeserializeOwned,
{
    let jobs = match jobs {
        None => None,
        Some(Value::Array(val)) => Some(val),
        _ => {
            // error!(
            //     "Decoding Message Failed: {:?}",
            //     "unknown result type for next message"
            // );
            None
        }
    };

    jobs.map(|values| {
        values
            .iter()
            .filter_map(|v| match v {
                Value::BulkString(data) => {
                    let inner = C::decode(data.to_vec())
                        .map_err(|e| (ErrorKind::IoError, "Decode error", e.into().to_string()))
                        .unwrap();
                    Some(inner)
                }
                _ => None,
            })
            .collect()
    })
}
