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
//! use actix_redis_jobs::{Consumer, Jobs, Producer, Queue, QueueActor, MessageGuard};

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

//! impl Handler<Jobs<DogoJobo>> for DogoActor {
//!     type Result = ();

//!     fn handle(&mut self, msg: Jobs<DogoJobo>, _: &mut Self::Context) -> Self::Result {
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
mod actor;
mod consumer;
mod error;
mod message; // Add message guard examples
mod producer;
// Am I doing this right?
pub use actor::QueueActor;
pub use actor::*;
pub use consumer::Consumer;
pub use consumer::Jobs;
pub use error::TaskError;
pub use producer::Producer;
pub use message::MessageGuard;
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
