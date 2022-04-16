use crate::queue::RedisQueue;
use crate::storage::RedisStorage;
use actix::prelude::*;
use apalis_core::{Error, Job, JobState, Producer, PushJob, Queue};

pub struct RedisProducer<J: Job> {
    pub(crate) queue: RedisQueue<J>,
    pub(crate) storage: RedisStorage,
}

impl<J: 'static + Job> RedisProducer<J> {
    pub fn start(queue: &Queue<J>, storage: &RedisStorage) -> Addr<Self> {
        RedisProducer::<J> {
            queue: RedisQueue::new(queue),
            storage: storage.clone(),
        }
        .start()
    }

    pub fn create(url: &str) -> Result<Addr<Self>, redis::RedisError> {
        let storage = RedisStorage::new(url)?;
        let queue = Queue::<J>::new();
        Ok(RedisProducer::start(&queue, &storage))
    }
}

impl<J: 'static + Job> Actor for RedisProducer<J> {
    type Context = Context<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        log::info!(
            "RedisProducer for Queue [{:?}] started",
            &self.queue.get_name()
        );
    }
}

/// Implementation of Actix Handler to push job.
impl<J: 'static + Job> Handler<PushJob> for RedisProducer<J> {
    type Result = ResponseFuture<Result<JobState, Error>>;

    fn handle(&mut self, mut msg: PushJob, _: &mut Self::Context) -> Self::Result {
        let conn = self.storage.clone();
        let push_job = redis::Script::new(include_str!("../lua/push_job.lua"));
        let job_data_hash = self.queue.job_data_hash.to_string();
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let signal_list = self.queue.signal_list.to_string();
        let fut = async move {
            let mut conn = conn.get_connection().await.unwrap();
            push_job
                .key(job_data_hash)
                .key(active_jobs_list)
                .key(signal_list)
                .arg(&msg.id.to_string())
                .arg(PushJob::encode(&msg).unwrap())
                .invoke_async(&mut conn)
                .await
                .map(|res: i8| {
                    if res > 0 {
                        msg.ack();
                        JobState::Acked
                    } else {
                        msg.reject();
                        JobState::Rejected
                    }
                })
                .map_err(|_e| Error::Failed)
        };
        Box::pin(fut)
    }
}

impl<J: 'static + Job> Producer for RedisProducer<J> {}
