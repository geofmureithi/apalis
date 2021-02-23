//! # Actix-redis-jobs
//! Simple and reliable background processing for Rust using Actix and Redis

//! ## Getting Started

//! To get started, just add to Cargo.toml

//! ```toml
//! [dependencies]
//! actix-redis-jobs = { version = "0.1" }
//! ```

//! ### Prerequisites

//! A running redis server is required.
//! You can quickly use docker:
//! ````bash
//! docker run --name some-redis -d redis
//! ````

//! ## Usage

//! ````rust
//! use actix::prelude::*;
//! use log::info;
//! use serde::{Deserialize, Serialize};
//! use actix_redis_jobs::{Consumer, Job, Producer, Queue, QueueActor, MessageGuard};

//! #[derive(Serialize, Deserialize, Debug, Message)]
//! #[rtype(result = "Result<(), ()>")]
//! struct DogoJobo {
//!     dogo: String,
//!     meme: String,
//! }

//! struct DogoActor;

//! impl Actor for DogoActor {
//!     type Context = Context<Self>;
//! }

//! impl Handler<Job<DogoJobo>> for DogoActor {
//!     type Result = ();

//!     fn handle(&mut self, msg: Job<DogoJobo>, _: &mut Self::Context) -> Self::Result {
//!         info!("Got sweet Dogo memes to post: {:?}", msg);
//!         let _guarded_messages: Vec<MessageGuard<DogoJobo>> = msg.0.into_iter().map(|m| {
//!             MessageGuard::new(m)
//!         }).collect();

//!         //! It should complain of dropping an unacked message
//!         //! msg.ack() should do the trick
//!     }
//! }

//! fn main() {
//!     let system = actix::System::new("test");
//!     std::env::set_var("RUST_LOG", "info");
//!     env_logger::init();
//!     Arbiter::spawn(async {
//!         let actor = QueueActor::new("redis://!127.0.0.1/", Queue::new("dogoapp")).await;
//!         let addr = Supervisor::start(move |_| actor);
//!         let p_addr = addr.clone();
//!         let dogo_processor = DogoActor.start();
//!         let consumer_id = String::from("doggo_handler_1");
//!         Supervisor::start(move |_| Consumer::new(addr, dogo_processor.recipient(), consumer_id));
//!         let producer = Producer::new(p_addr);
//!         let task = DogoJobo {
//!             dogo: String::from("Test Dogo Meme"),
//!             meme: String::from("https://!i.imgur.com/qgpUDVH.jpeg"),
//!         };
//!         producer.push_job(task).await;
//!     });
//!     let _s = system.run();
//! }

//! ````
mod consumer;
mod error;
mod message;
mod worker;
mod storage;
mod producer;
mod queue;

pub use producer::{Producer, PushJob, JobStatus, ScheduleJob};
pub use consumer::Consumer;
pub use consumer::Job;
pub use error::TaskError;
pub use consumer::JobHandler;
pub use consumer::JobResult;
pub use consumer::JobContext;
pub use worker::Worker;
pub use storage::redis::RedisStorage;
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use actix::{Actor, Arbiter, System};
//     use futures::FutureExt;

//     async fn actix_redis_actor(url: &str) -> QueueActor {
//         QueueActor::new(url, Queue::new("testapp"))
//     }

//     #[test]
//     fn lua_actor_basic() {
//         let system = System::new("test");

//         Arbiter::spawn(async {
//             let queue_addr = actix_redis_actor(r#"redis://127.0.0.1/"#).await.start();
//             let l = queue_addr.send(FetchJob {
//                 consumer_id: String::from("testapp1"),
//             });
//             l.map(|res| {
//                 assert_eq!(res.unwrap().unwrap(), vec!());
//                 System::current().stop();
//             })
//             .await;
//         });
//         system.run();
//     }
//     #[test]
//     fn it_works() {
//         assert_eq!(2 + 2, 4);
//     }
// }
