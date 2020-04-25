# Actix-redis-jobs
Simple and reliable background processing for Rust using Actix and Redis

## Getting Started

To get started, just add to Cargo.toml 

```toml
[dependencies]
actix-redis-jobs = { version = "0.1" }
```

### Prerequisites

A running redis server is required.
You can quickly use docker:
````bash
docker run --name some-redis -d redis
````

## Usage

````rust
use actix::prelude::*;
use log::info;
use serde::{Deserialize, Serialize};
use actix_redis_jobs::{Consumer, Jobs, Producer, Queue, QueueActor, MessageGuard};

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
        info!("Got sweet Dogo memes to post: {:?}", msg);
        let _guarded_messages: Vec<MessageGuard<DogoJobo>> = msg.0.into_iter().map(|m| {
            MessageGuard::new(m)
        }).collect();

        // It should complain of dropping an unacked message
        // msg.ack() should do the trick
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

````

## Built With

* [Actix](https://actix.rs) - Actor framework for Rust
* [Redis-Rs](https://github.com/mitsuhiko/redis-rs) - Redis library for rust 

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/geofmureithi/actix-jobs/tags). 

## Authors

* **Njuguna Mureithi** - *Initial work* - [Njuguna Mureithi](https://github.com/geofmureithi)

See also the list of [contributors](https://github.com/geofmureithi/actix-jobs/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Inspiration: This project is heavilly inspired by [Curlyq](https://github.com/mcmathja/curlyq) which is written in GoLang
