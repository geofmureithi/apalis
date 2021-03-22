//! # actix-redis-jobs
//! Simple and reliable background processing for Rust using Actix and Redis

//! ## Getting Started

//! To get started, just add to Cargo.toml

//! ```toml
//! [dependencies]
//! actix-redis-jobs = { version = "0.2.0-beta.1" }
//! ```

//! ### Prerequisites

//! A running redis server is required.
//! You can quickly use docker:
//! ````bash
//! docker run --name some-redis -d redis
//! ````

//! ## Usage

//! ````rust
/// use actix::prelude::*;
/// use futures::future::BoxFuture;
/// use log::info;
/// use serde::{Deserialize, Serialize};
/// use std::sync::Arc;
/// use std::sync::Mutex;

/// struct MyData {
///     counter: usize,
/// }

/// use actix_redis_jobs::{
///     JobContext, JobHandler, JobResult, Producer, RedisConsumer, RedisStorage, ScheduleJob,
///     WorkManager,
/// };

/// #[derive(Serialize, Deserialize, Message, Clone)]
/// #[rtype(result = "()")]
/// enum Math {
///     Sum(isize, isize),
///     Multiply(isize, isize),
/// }

/// impl JobHandler for Math {
///     fn handle(&self, _ctx: &JobContext) -> BoxFuture<JobResult> {
///         let counter = _ctx.data_opt::<Arc<Mutex<MyData>>>().unwrap();
///         let mut data = counter.lock().unwrap();
///         data.counter += 1;
///         info!("Done like {} jobs", data.counter);
///         let fut = async move {
///             match self {
///                 Math::Sum(first, second) => {
///                     info!(
///                         "Sum result for {} and {} is {}",
///                         first,
///                         second,
///                         first + second
///                     );
///                     JobResult::Result(Ok(()))
///                 }
///                 Math::Multiply(first, second) => {
///                     info!(
///                         "Multiply result for {} and {} is {}",
///                         first,
///                         second,
///                         first * second
///                     );
///                     JobResult::Result(Ok(()))
///                 }
///             }
///         };
///         Box::pin(fut)
///     }
/// }

/// #[actix_rt::main]
/// async fn main() {
///     std::env::set_var("RUST_LOG", "info");
///     env_logger::init();
///     let storage = RedisStorage::new("redis://127.0.0.1/");
///     let producer = Producer::start(&storage, "math");

///     let multiply = Math::Multiply(9, 8);
///     producer.do_send(multiply); //Handled instantly

///     let sum = Math::Sum(1, 2);
///     let scheduled = ScheduleJob::new(sum).in_minutes(1); //Scheduled into the future
///     producer.do_send(scheduled);

///     let counter = Arc::new(Mutex::new(MyData { counter: 0 }));
///     WorkManager::create(move |worker| {
///         worker.consumer(
///             RedisConsumer::<Math>::new(&storage, "math")
///                 .data(counter.clone()) //Actix ideas
///                 .workers(2),
///         )
///     })
///     .run()
///     .await;
/// }

///````
mod consumer;
mod error;
mod message;
mod producer;
mod queue;
mod storage;
mod worker;

pub use consumer::Consumer;
pub use consumer::Job;
pub use consumer::JobContext;
pub use consumer::JobHandler;
pub use consumer::JobResult;
pub use consumer::RedisConsumer;
pub use error::TaskError;
pub use producer::{JobStatus, Producer, PushJob, ScheduleJob};
pub use storage::redis::RedisStorage;
pub use worker::WorkManager;

#[cfg(test)]
mod tests {

    #[test]
    fn actor_jobs_basic() {
    
    }
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
