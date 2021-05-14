use crate::consumer::RedisConsumer;
use actix::prelude::*;
use apalis::{Error, Job, JobHandler, MessageEncodable, PushJob};
use chrono::prelude::*;
/// Actix message implements request Redis to retry jobs
#[derive(Message)]
#[rtype(result = "Result<Option<i8>, Error>")]
pub struct RetryJob {
    job: PushJob,
    retry_at: DateTime<Utc>,
}

impl RetryJob {
    pub fn now(job: PushJob) -> Self {
        RetryJob {
            job,
            retry_at: DateTime::from(Local::now())
        }
    }
}

/// Implementation of Actix Handler for retrying jobs
impl<J: 'static + Unpin + JobHandler<Self>> Handler<RetryJob> for RedisConsumer<J>
where
    J: Job,
{
    type Result = ResponseFuture<Result<Option<i8>, Error>>;

    fn handle(&mut self, msg: RetryJob, _: &mut Self::Context) -> Self::Result {
        let conn = self.queue.storage.clone();
        let retry_jobs = redis::Script::new(include_str!("../../lua/retry_job.lua"));
        let inflight_set = format!("{}:{}", &self.queue.inflight_jobs_prefix, &self.id());
        let scheduled_jobs_set = self.queue.scheduled_jobs_set.to_string();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let id = &msg.job.id.clone();
        let id = id.to_string();
        let message = MessageEncodable::encode_message(&msg.job).unwrap();
        let fut = async move {
            let mut conn = conn.get_connection().await.unwrap();
            retry_jobs
                .key(inflight_set)
                .key(scheduled_jobs_set)
                .key(job_data_hash)
                .arg(id)
                .arg(msg.retry_at.timestamp())
                .arg(message) // This needs to be new job data
                .invoke_async(&mut conn)
                .await
                .map_err(|_| Error::Failed)
        };
        Box::pin(fut)
    }
}
