use apalis_core::{Job, JobHandler, Error, PushJob};
use crate::consumer::RedisConsumer;
use actix::prelude::*;

/// Actix message implements request Redis to ack job
#[derive(Message, Debug)]
#[rtype(result = "Result<Option<bool>, Error>")]
pub struct AckJob {
    job_id: String,
}

impl AckJob {
    pub fn from(job: &PushJob) -> Self {
        AckJob {
            job_id: job.id.to_string(),
        }
    }
}
/// Implementation of Actix Handler for Get message.
impl<J: 'static + Unpin + JobHandler<Self>> Handler<AckJob> for RedisConsumer<J>
where
    J: Job,
{
    type Result = ResponseFuture<Result<Option<bool>, Error>>;

    fn handle(&mut self, msg: AckJob, _: &mut Self::Context) -> Self::Result {
        let conn = self.queue.storage.clone();
        let ack_job = redis::Script::new(include_str!("../../lua/ack_job.lua"));
        let inflight_set = format!("{}:{}", &self.queue.inflight_jobs_prefix, &self.id());
        let data_hash = format!("{}", &self.queue.job_data_hash);
        let fut = async move {
            let mut conn = conn.get_connection().await.unwrap();
            ack_job
                .key(inflight_set)
                .key(data_hash)
                .arg(msg.job_id)
                .invoke_async(&mut conn)
                .await
                .map_err(|_| Error::Failed)
        };
        Box::pin(fut)
    }
}
