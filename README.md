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

```bash
docker run --name some-redis -d redis
```

## Usage

```rust
use actix::prelude::*;
use log::info;
use serde::{Deserialize, Serialize};
use futures::future::BoxFuture;

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
    let sum = Math::Sum(1, 2);
    let multiply = Math::Multiply(9, 8);
    let scheduled = ScheduleJob::new(sum).in_minutes(1);
    producer.do_send(scheduled);
    producer.do_send(multiply);

    WorkManager::create(move |worker| {
        worker.consumer(RedisConsumer::<Math>::new(&storage, "math").workers(2))
    })
    .run()
    .await;
}
```

## Built With

- [Actix](https://actix.rs) - Actor framework for Rust
- [Redis-Rs](https://github.com/mitsuhiko/redis-rs) - Redis library for rust

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/geofmureithi/actix-jobs/tags).

## Authors

- **Njuguna Mureithi** - _Initial work_ - [Njuguna Mureithi](https://github.com/geofmureithi)

See also the list of [contributors](https://github.com/geofmureithi/actix-jobs/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

- Inspiration: This project is inspired by [Curlyq](https://github.com/mcmathja/curlyq) which is written in GoLang
