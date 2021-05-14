use crate::messages::fetch::FetchJobStream;
use crate::messages::heartbeat::HeartBeatStream;
use crate::messages::register::RegisterConsumer;
use crate::messages::schedule::ScheduleStream;
use crate::messages::stop::Stop;
use crate::queue::RedisQueue;
use crate::storage::RedisStorage;
use crate::RedisProducer;
use actix::clock::{interval_at, Instant};
use actix::prelude::*;
use actix::Actor;
use actix::Context;
use apalis_core::{Consumer, Job, JobContext, JobHandler, Queue};
use log::*;
use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

pub type RedisJobContext<T> = JobContext<RedisConsumer<T>>;

#[derive(Clone)]
pub struct RedisConsumer<J: 'static + Job + JobHandler<RedisConsumer<J>>> {
    pub(crate) data: Arc<Mutex<JobContext<Self>>>,
    pub(crate) queue: RedisQueue<J>,
    id: String,
    typeid: PhantomData<J>,
}

impl<J: Job + JobHandler<RedisConsumer<J>>> RedisConsumer<J> {
    pub fn new(queue: &Queue<J, RedisStorage>) -> Self {
        let redis_queue = RedisQueue::new(queue);
        RedisConsumer {
            data: Arc::new(Mutex::new(JobContext::new())),
            queue: redis_queue,
            id: format!("RedisConsumer[{:?}]", uuid::Uuid::new_v4()),
            typeid: PhantomData,
        }
    }
    pub fn id(&self) -> &String {
        &self.id
    }

    pub fn data<D: Any + Send + Sync>(self, data: D) -> Self {
        self.data.lock().unwrap().insert::<D>(data);
        self
    }

    pub fn create(url: &str) -> Result<Self, redis::RedisError> {
        let storage = RedisStorage::new(url)?;
        let queue = Queue::<J, RedisStorage>::new(&storage);
        Ok(RedisConsumer::new(&queue))
    }

    pub fn build_producer(&self) -> Addr<RedisProducer<J>> {
        let storage = &self.queue.storage;
        let queue = Queue::new(storage);
        RedisProducer::start(&queue)
    }
}

impl<J: 'static + JobHandler<Self>> Consumer for RedisConsumer<J> where J: Job {}

impl<J: 'static + JobHandler<Self>> Actor for RedisConsumer<J>
where
    J: Job,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        debug!("Starting consumer in Thread {:?}", std::thread::current());
        let queue = self.queue.get_name().clone();
        let consumer_id = self.id.clone();
        let addr = ctx.address();
        let fut = async move {
            let reg = addr.send(RegisterConsumer).await;
            match reg {
                Ok(Ok(Some(true))) => {
                    info!(
                        "Consumer: {:} for Queue [{:}] successfully registered",
                        consumer_id, queue
                    );
                }
                _ => {
                    addr.send(Stop).await.unwrap();
                }
            };
        };
        let fut = actix::fut::wrap_future::<_, Self>(fut);
        ctx.spawn(fut);

        let start = Instant::now() + Duration::from_millis(50);
        ctx.add_stream(HeartBeatStream::new(interval_at(
            start,
            Duration::from_secs(30),
        )));
        debug!("Added Heartbeat for RedisConsumer [{:?}]", self.id);

        ctx.add_stream(ScheduleStream::new(interval_at(
            start,
            Duration::from_secs(10),
        )));
        debug!("Added Scheduling for RedisConsumer [{:?}]", self.id);
        ctx.add_stream(FetchJobStream::new(interval_at(
            start,
            Duration::from_millis(250),
        )));
        debug!("Added Fetcher for RedisConsumer [{:?}]", self.id);
        // ctx.add_stream(ReenqueueOrphanedStream {
        //     interval: interval_at(start, Duration::from_secs(30)),
        // });
    }
}

impl<J: 'static + Unpin + JobHandler<Self>> actix::Supervised for RedisConsumer<J>
where
    J: Job,
{
    fn restarting(&mut self, _: &mut Context<RedisConsumer<J>>) {
        info!("Restarting RedisConsumer: [{:?}]", self.id);
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use apalis::Job;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct TestJob;

    impl Job for TestJob {
        type Result = ();
    }

    impl JobHandler<RedisConsumer<Self>> for TestJob {
        type Result = ();
        fn handle(self, _: &mut JobContext<RedisConsumer<Self>>) {
            todo!()
        }
    }

    #[actix_rt::test]
    async fn test_handles_job() {
        let storage = RedisStorage::new("redis://127.0.0.1/").unwrap();
        let queue = Queue::<TestJob, RedisStorage>::new(&storage);
        let addr = Actor::create(|_ctx| RedisConsumer::new(&queue));
        assert!(addr.connected())
    }
}
