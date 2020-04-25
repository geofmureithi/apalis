use actix::prelude::*;
use log::info;

use serde::{Deserialize, Serialize};

extern crate actix_redis_jobs;

use actix_redis_jobs::{Consumer, Jobs, Producer, Queue, QueueActor};

#[derive(Serialize, Deserialize, Debug, Message)]
#[rtype(result = "Result<(), ()>")]
struct DogoJobo {
    dogo: String,
    meme: String,
}

struct DogoActor;

impl Actor for DogoActor {
    type Context = Context<Self>;
}

impl Handler<Jobs<DogoJobo>> for DogoActor {
    type Result = ();

    fn handle(&mut self, msg: Jobs<DogoJobo>, _: &mut Self::Context) -> Self::Result {
        info!("Got sweet Dogo memes to post: {:?}", msg)
    }
}

fn main() {
    let system = actix::System::new("test");
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
    Arbiter::spawn(async {
        let actor = QueueActor::new("redis://127.0.0.1/", Queue::new("dogoapp")).await;
        let addr = Supervisor::start(move |_| actor);
        let p_addr = addr.clone();
        let dogo_processor = DogoActor.start();
        let consumer_id = String::from("doggo_handler_1");
        Supervisor::start(move |_| Consumer::new(addr, dogo_processor.recipient(), consumer_id));
        let producer = Producer::new(p_addr);
        let task = DogoJobo {
            dogo: String::from("Test Dogo Meme"),
            meme: String::from("https://i.imgur.com/qgpUDVH.jpeg"),
        };
        producer.push_job(task).await;
    });
    let _s = system.run();
}
