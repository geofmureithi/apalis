use crate::consumer::RedisConsumer;
use actix::prelude::*;
use apalis_core::{Error, Job, JobHandler};
use chrono::Utc;

/// Actix message implements registering the consumer
#[derive(Message, Debug)]
#[rtype(result = "Result<Option<bool>, Error>")]
pub struct RegisterConsumer;

impl<J: 'static + Unpin + JobHandler<Self>> Handler<RegisterConsumer> for RedisConsumer<J>
where
    J: Job,
{
    type Result = ResponseFuture<Result<Option<bool>, Error>>;

    fn handle(&mut self, _msg: RegisterConsumer, _: &mut Self::Context) -> Self::Result {
        let conn = self.storage.clone();
        let register_consumer = redis::Script::new(include_str!("../../lua/register_consumer.lua"));
        let inflight_set = format!("{}:{}", &self.queue.inflight_jobs_prefix, self.id());
        let consumers_set = self.queue.consumers_set.to_string();
        let timestamp = Utc::now().timestamp();
        let fut = async move {
            let mut conn = conn.get_connection().await.unwrap();
            register_consumer
                .key(consumers_set)
                .arg(timestamp)
                .arg(inflight_set)
                .invoke_async(&mut conn)
                .await
                .map_err(|_| Error::Failed)
        };
        Box::pin(fut)
    }
}
