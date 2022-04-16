# Apalis [![Build Status](https://travis-ci.org/geofmureithi/apalis.svg?branch=master)](https://travis-ci.org/geofmureithi/apalis)

Simple and reliable background processing for Rust using Actix actors. Apalis currently supports Redis as a store, with SQlite, PostgresSQL and MySQL in the pipeline.

## Getting Started

To get started, just add to Cargo.toml

```toml
[dependencies]
apalis = { version = "0.2", features = ["redis"] }
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
use apalis::{
    redis::{RedisConsumer, RedisProducer, RedisStorage}
    Job, JobContext, JobFuture, JobHandler, Queue, Worker
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Debug)]
pub enum MathError {
    InternalError,
}

#[derive(Serialize, Deserialize)]
pub enum Math {
    Add(u64, u64),
    Fibonacci(u64),
}

impl Job for Math {
    type Result = Result<u64, MathError>;
}

impl JobHandler<RedisConsumer<Math>> for Math {
    type Result = JobFuture<Result<u64, MathError>>;
    fn handle(
        self,
        ctx: &mut JobContext<RedisConsumer<Math>>,
    ) -> JobFuture<Result<u64, MathError>> {
        let data = ctx.data_opt::<Arc<Mutex<MathCounter>>>().unwrap();
        let mut data = data.lock().unwrap();
        data.counter += 1;
        match self {
            Math::Add(first, second) => Box::pin(async move { Ok(first + second) }),
            Math::Fibonacci(num) => {
                let addr = ctx.data_opt::<Addr<FibonacciActor>>().unwrap().clone();
                Box::pin(async move {
                    addr.send(Fibonacci(num))
                        .await
                        .map_err(|_e| MathError::InternalError)?
                })
            }
        }
    }
}

fn produce_jobs(queue: &Queue<Math, RedisStorage>) {
    let producer = RedisProducer::start(queue);
    producer.do_send(Math::Add(1, 2).into());
    producer.do_send(Math::Fibonacci(9).into());
}



#[actix_rt::main]
async fn main() {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
    let storage = RedisStorage::new("redis://127.0.0.1/").unwrap();
    let queue = Queue::<Math, RedisStorage>::new(&storage);

    //This can be in another part of the program
    produce_jobs(&queue);

    let counter = Arc::new(Mutex::new(MathCounter { counter: 0 }));
    let addr = SyncArbiter::start(2, || FibonacciActor); //Get the address of another actor
    Worker::new()
        .register_with_threads(2, move || {
            RedisConsumer::new(&queue)
                .data(counter.clone())
                .data(addr.clone())
        })
        .run()
        .await;
}
```

````rust
use serde::{Deserialize, Serialize};

use std::cell::{RefCell, RefMut};

/// Shorthand version of `RefMut<T>`, if you don't want to import `RefMut`.
pub type WareArg<'a, T> = RefMut<'a, T>;

/// A middleware chain.
pub struct Ware<T> {
    /// The internal list of middleware functions.
    pub fns: Vec<Box<dyn Fn(WareArg<T>) -> ()>>,
}

impl<T> Ware<T> {
    /// Create a new middleware chain with a given type.
    ///
    /// # Example
    /// ```
    /// use ware::Ware;
    /// let mut chain: Ware<String> = Ware::new();
    /// ```
    pub fn new() -> Ware<T> {
        let vec: Vec<Box<dyn Fn(WareArg<T>) -> ()>> = Vec::new();
        Ware { fns: vec }
    }

    /// Add a new middleware function to the internal function list. This function
    /// must be of the `Fn` trait, take a `WareArg<T>` and return a unit struct (aka. nothing).
    /// It also has to be boxed for memory safety reasons.
    ///
    /// # Example
    /// ```
    /// use ware::Ware;
    /// let mut chain: Ware<String> = Ware::new();
    /// chain.wrap(Box::new(|mut st| {
    ///     st.push('a');
    /// }))
    /// ```
    pub fn wrap(&mut self, func: Box<dyn Fn(WareArg<T>) -> ()>) {
        self.fns.push(func);
    }

    /// Run the registered middleware functions with the given value to pass
    /// through. Returns whatever the passed value will be after the last
    /// middleware function runs.
    pub fn run(&self, arg: T) -> T {
        let ware_arg = RefCell::new(arg);
        self.fns.iter().for_each(|func| func(ware_arg.borrow_mut()));
        ware_arg.into_inner()
    }
}

// #[cfg(test)]
// mod tests {
// 	use std::ops::Add;

// 	use super::*;

// 	// #[test]
// 	// fn it_works() {
// 	// 	let value = 1;
// 	// 	let mut w: Ware<i32> = Ware::new();
// 	// 	w.wrap(Box::new(|mut num| {
// 	// 		*num = num.add(1);
// 	// 	}));
// 	// 	assert_eq!(w.run(value), 2);
// 	// }
// }

pub struct WorkerContext;

trait Job {
    fn handle(&self, ctx: &WorkerContext);
}

// trait Middleware {
//     fn wrap(&self)
// }

trait Queue<T: Job> {
    fn run(&self);
}

struct InMemoryQueue;

impl Queue<Email> for InMemoryQueue {
    fn run(&self) {}
}

struct Worker {
    queues: Vec<Box<dyn Job>>,
    context: WorkerContext,
}

impl Worker {
    fn register<J: 'static + Job, Q: Queue<J>>(mut self, job: J, queue: Q) -> Self {
        self.queues.push(Box::new(job));
        self
    }
}

struct Request<T: Serialize>(T);

// impl<T: Serialize> Job for Request<T> {
//     fn handle(&self, _ctx: &WorkerContext){
//         // TODO:
//     }
// }

#[derive(Serialize, Deserialize)]
struct Email {
    to: String,
}

fn email_handler(req: Request<Email>, ctx: &WorkerContext) {}

impl Job for Email {
    fn handle(&self, _ctx: &WorkerContext) {
        // TODO:
    }
}

fn main() {
    let worker = Worker {
        queues: Vec::new(),
        context: WorkerContext,
    }
    .register(
        Email {
            to: "test".to_owned(),
        },
        InMemoryQueue,
    );
}

````

## Built On

- [actix](https://actix.rs) - Actor framework for Rust
- [redis-rs](https://github.com/mitsuhiko/redis-rs) - Redis library for rust
- [sqlx](https://github.com/launchbadge/sqlx) - The Rust SQL Toolkit

## Roadmap

v 0.3

- [ ] Standardize API (Storage, Worker, Data, Middleware, Context )
- [ ] Introduce SQL, specifically
- [ ] Basic Web API Interface

v 0.2

- [x] Redis Example
- [x] Actix Web Example

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/geofmureithi/apalis/tags).

## Authors

- **Njuguna Mureithi** - _Initial work_ - [Njuguna Mureithi](https://github.com/geofmureithi)

See also the list of [contributors](https://github.com/geofmureithi/apalis/contributors) who participated in this project.

It was formerly `actix-redis-jobs` and if you want to use the crate name please contact me.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

- Inspiration: The redis part of this project is heavily inspired by [Curlyq](https://github.com/mcmathja/curlyq) which is written in GoLang
