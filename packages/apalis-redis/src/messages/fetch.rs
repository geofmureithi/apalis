use crate::consumer::RedisConsumer;
use crate::messages::ackjob::AckJob;
use crate::messages::retry::RetryJob;
use actix::clock::Instant;
use actix::prelude::*;
use actix_rt::time::Interval;
use apalis::{Error, Job, JobHandler, JobState, MessageDecodable, PushJob};
use log::{*};
use redis::Value;
use std::pin::Pin;
use std::task::{Context as StdContext, Poll};
/// Actix message implements request redis to fetch jobs
#[derive(Debug)]
pub struct FetchJob;

pub struct FetchJobStream {
    interval: Interval,
}

impl FetchJobStream {
    pub fn new(interval: Interval) -> Self {
        FetchJobStream { interval }
    }
}

impl Stream for FetchJobStream {
    type Item = FetchJob;

    fn poll_next(self: Pin<&mut Self>, cx: &mut StdContext<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .interval
            .poll_tick(cx)
            .map(|_| Some(FetchJob))
    }
}

/// Implementation of Actix Handler fetching jobs.
impl<J: 'static + Unpin + JobHandler<Self> + Send> StreamHandler<FetchJob> for RedisConsumer<J>
where
    J: Job,
{
    fn handle(&mut self, _msg: FetchJob, ctx: &mut Self::Context) {
        let conn = self.queue.storage.clone();
        let fetch_jobs = redis::Script::new(include_str!("../../lua/get_jobs.lua"));
        let consumers_set = self.queue.consumers_set.to_string();
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let inflight_set = format!("{}:{}", self.queue.inflight_jobs_prefix, self.id());
        let signal_list = self.queue.signal_list.to_string();
        let queue_name = self.queue.get_name().clone();
        let addr = ctx.address();
        let context = self.data.clone();
        let fut = async move {
            let mut conn = conn.get_connection().await.unwrap();
            let res: Result<Vec<Value>, Error> = fetch_jobs
                .key(consumers_set)
                .key(active_jobs_list)
                .key(&inflight_set)
                .key(job_data_hash)
                .key(signal_list)
                .arg("1") // Fetch one Job at a time
                .arg(&inflight_set)
                .invoke_async(&mut conn)
                .await
                .map_err(|_| Error::Failed);
            match res {
                Ok(jobs) => {
                    let job = jobs.get(0);
                    let job = match job {
                        job @ Some(Value::Data(_)) => job.unwrap(),
                        None => {
                            return debug!("No new jobs found");
                        }
                        _ => {
                            return error!(
                                "Decoding Message Failed: {:?}",
                                "unknown result type for next message"
                            )
                        }
                    };

                    let bytes = match &job {
                        Value::Data(v) => v,
                        _ => {
                            return error!("Decoding Message Failed: {:?}", "Expected Redis String")
                        }
                    };

                    match PushJob::decode_message(&bytes) {
                        Err(e) => {
                            error!("Decoding Message Failed: {:?}", e);
                        }
                        Ok(mut job) => {
                            let start = Instant::now();
                            job.handle::<Self, J>(&mut context.lock().unwrap()).await;
                            match job.state() {
                                JobState::Acked => {
                                    let ack = AckJob::from(&job);
                                    let _res = addr.send(ack).await;
                                }

                                JobState::Rejected => {
                                    if J::retries() < job.retries {
                                        let _res = addr.send(RetryJob::now(job)).await;
                                    } else {
                                        debug!(
                                            "Unable to retry the job [{}] after {} retries",
                                            job.id, job.retries
                                        )
                                    }
                                }
                                JobState::Unacked => {
                                    warn!("Something went wrong. Dropping Unacked job [{}]", job.id)
                                }
                            }
                            let duration = start.elapsed();
                            debug!(
                                "[{}]: Time elapsed in handling job is: {:?}",
                                queue_name, duration
                            );
                        }
                    };
                }
                Err(e) => {
                    debug!("Unable to Fetch jobs, Error: {:?}", e);
                }
            }
        };
        let fut = actix::fut::wrap_future::<_, Self>(fut);
        ctx.spawn(fut);
    }
}
