#[macro_use]
extern crate criterion;

use actix::prelude::*;
use apalis::{
    redis::{RedisConsumer, RedisJobContext, RedisProducer, RedisStorage},
    Job, JobFuture, JobHandler, Queue, Worker,
};
use criterion::async_executor::FuturesExecutor;
use criterion::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::Mutex;

struct Fibonacci(pub u64);

impl Message for Fibonacci {
    type Result = Result<u64, MathError>;
}

struct SyncActor;

impl Actor for SyncActor {
    type Context = SyncContext<Self>;
}

struct MathCounter {
    counter: usize,
}

impl Handler<Fibonacci> for SyncActor {
    type Result = Result<u64, MathError>;

    fn handle(&mut self, msg: Fibonacci, _: &mut Self::Context) -> Self::Result {
        if msg.0 == 0 {
            Err(MathError::InternalError)
        } else if msg.0 == 1 {
            Ok(1)
        } else {
            let mut i = 0;
            let mut sum = 0;
            let mut last = 0;
            let mut curr = 1;
            while i < msg.0 - 1 {
                sum = last + curr;
                last = curr;
                curr = sum;
                i += 1;
            }
            Ok(sum)
        }
    }
}

#[derive(Debug)]
pub enum MathError {
    InternalError,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub enum Math {
    Add(u64, u64),
    Fibonacci(u64),
}

impl Job for Math {
    type Result = Result<u64, MathError>;
}

impl JobHandler<RedisConsumer<Math>> for Math {
    type Result = JobFuture<Result<u64, MathError>>;
    fn handle(self, ctx: &mut RedisJobContext<Math>) -> JobFuture<Result<u64, MathError>> {
        let data = ctx.data_opt::<Arc<Mutex<MathCounter>>>().unwrap();
        let mut data = data.lock().unwrap();
        data.counter += 1;
        match self {
            Math::Add(first, second) => Box::pin(async move { Ok(first + second) }),
            Math::Fibonacci(num) => {
                let addr = ctx.data_opt::<Addr<SyncActor>>().unwrap().clone();
                Box::pin(async move {
                    addr.send(Fibonacci(num))
                        .await
                        .map_err(|_e| MathError::InternalError)?
                })
            }
        }
    }
}

fn produce_jobs(queue: &Queue<Math>, storage: &RedisStorage) {
    let producer = RedisProducer::start(queue, storage);
    producer.do_send(Math::Add(1, 2).into());
    producer.do_send(Math::Fibonacci(9).into());
}
async fn redis(queue: &Queue<Math>, storage: &RedisStorage) {
    //This can be in another part of the program
    produce_jobs(&queue, &storage);
}

fn bench(c: &mut Criterion) {
    let sys = actix::System::new("test");
    let storage = RedisStorage::new("redis://127.0.0.1/").unwrap();
    let inner_storage = storage.clone();
    let queue = Queue::<Math>::new();
    let inner_queue = queue.clone();
    let counter = Arc::new(Mutex::new(MathCounter { counter: 0 }));
    let addr = SyncArbiter::start(2, || SyncActor);
    let _worker = Worker::new().register_with_threads(2, move || {
        RedisConsumer::new(&queue, &storage)
            .data(counter.clone())
            .data(addr.clone())
    });
    c.bench_function("apalis", move |b| {
        b.to_async(FuturesExecutor)
            .iter(|| async { redis(&inner_queue, &inner_storage).await })
    });

    let _ = sys.run();
}

criterion_group!(benches, bench);
criterion_main!(benches);
