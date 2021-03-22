use actix::prelude::*;
use futures::future::BoxFuture;
use log::info;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::Mutex;

struct MyData {
    counter: usize,
}

use actix_redis_jobs::{
    JobContext, JobHandler, JobResult, Producer, RedisConsumer, RedisStorage, ScheduleJob,
    WorkManager,
};

#[derive(Serialize, Deserialize, Message, Clone)]
#[rtype(result = "()")]
enum Math {
    Sum(isize, isize),
    Multiply(isize, isize),
}

impl JobHandler for Math {
    fn handle(&self, _ctx: &JobContext) -> BoxFuture<JobResult> {
        let counter = _ctx.data_opt::<Arc<Mutex<MyData>>>().unwrap();
        let mut data = counter.lock().unwrap();
        data.counter += 1;
        info!("Done like {} jobs", data.counter);
        let fut = async move {
            match self {
                Math::Sum(first, second) => {
                    info!(
                        "Sum result for {} and {} is {}",
                        first,
                        second,
                        first + second
                    );
                    JobResult::Result(Ok(()))
                }
                Math::Multiply(first, second) => {
                    info!(
                        "Multiply result for {} and {} is {}",
                        first,
                        second,
                        first * second
                    );
                    JobResult::Result(Ok(()))
                }
            }
        };
        Box::pin(fut)
    }
}

#[actix_rt::main]
async fn main() {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
    let storage = RedisStorage::new("redis://127.0.0.1/");
    let producer = Producer::start(&storage, "math");

    let multiply = Math::Multiply(9, 8);
    producer.do_send(multiply); //Handled instantly

    let sum = Math::Sum(1, 2);
    let scheduled = ScheduleJob::new(sum).in_minutes(1); //Scheduled into the future
    producer.do_send(scheduled);

    let counter = Arc::new(Mutex::new(MyData { counter: 0 }));
    WorkManager::create(move |worker| {
        worker.consumer(
            RedisConsumer::<Math>::new(&storage, "math")
                .data(counter.clone()) //Actix ideas
                .workers(2),
        )
    })
    .run()
    .await;
}
