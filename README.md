# Apalis [![Build Status](https://travis-ci.org/geofmureithi/apalis.svg?branch=master)](https://travis-ci.org/geofmureithi/apalis)

Apalis is a simple, extensible multithreaded background job processing library for Rust.

## Features

- Simple and predictable job handling model.
- Jobs handlers with a macro free API.
- Take full advantage of the [`tower`] ecosystem of
  middleware, services, and utilities.
- Workers take full of the actor model.
- Fully Tokio compatible.
- Optional Web interface to help you manage your jobs.

Apalis job processing is powered by [`tower::Service`] which means you have access to the [`tower`] and [`tower-http`] middleware.

Apalis has support for

- Redis
- SQlite
- PostgresSQL
- MySQL
- Bring Your Own Job Source eg Cron or Twitter streams

## Getting Started

To get started, just add to Cargo.toml

```toml
[dependencies]
apalis = { version = "0.3.1", features = ["redis"] }
```

## Usage

```rust
use apalis::{redis::RedisStorage, JobError, JobRequest, JobResult, WorkerBuilder, Storage, Monitor, JobContext};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct Email {
    to: String,
}

async fn email_service(job: Email, _ctx: JobContext) -> Result<JobResult, JobError> {
    Ok(JobResult::Success)
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();
    let redis = std::env::var("REDIS_URL").expect("Missing env variable REDIS_URL");
    let storage = RedisStorage::new(redis).await.unwrap();
    Monitor::new()
        .register_with_count(2, move || {
            WorkerBuilder::new(storage.clone())
                .build_fn(email_service)
        })
        .run()
        .await
}

```

Then

```rust
//This can be in another part of the program or another application
async fn produce_route_jobs(storage: &RedisStorage<Email>) {
    let mut storage = storage.clone();
    storage
        .push(Email {
            to: "test@example.com".to_string(),
        })
        .await
        .unwrap();
}

```

### Web UI

If you are running [Apalis Board](https://github.com/geofmureithi/apalis-board), you can easily manage your jobs. See a working [Rest API here](https://github.com/geofmureithi/apalis/tree/master/examples/rest-api)

![UI](https://github.com/geofmureithi/apalis-board/raw/master/screenshots/workers.png)

## Feature flags

- _tracing_ (enabled by default) — Support Tracing 👀
- _redis_ — Include redis storage
- _postgres_ — Include Postgres storage
- _sqlite_ — Include SQlite storage
- _mysql_ — Include MySql storage
- _sentry_ — Support for Sentry exception and performance monitoring
- _prometheus_ — Support Prometheus metrics
- _retry_ — Support direct retrying jobs
- _timeout_ — Support timeouts on jobs
- _limit_ — 💪 Limit the amount of jobs
- _filter_ — Support filtering jobs based on a predicate
- _extensions_ — Add a global extensions to jobs

## Storage Comparison

Since we provide a few storage solutions, here is a table comparing them:

| Feature         | Redis | Sqlite | Postgres | Sled | Mysql | Mongo |
| :-------------- | :---: | :----: | :------: | :--: | ----- | ----- |
| Scheduled jobs  |   ✓   |   ✓    |    ✓     |  x   | ✓     | x     |
| Retryable jobs  |   ✓   |   ✓    |    ✓     |  x   | ✓     | x     |
| Persistence     |   ✓   |   ✓    |    ✓     |  x   | ✓     | x     |
| Rerun Dead jobs |   ✓   |   ✓    |    ✓     |  x   | \*    | x     |

## Thanks to

- [`tower`] - Tower is a library of modular and reusable components for building robust networking clients and servers.
- [redis-rs](https://github.com/mitsuhiko/redis-rs) - Redis library for rust
- [sqlx](https://github.com/launchbadge/sqlx) - The Rust SQL Toolkit

## Roadmap

v 0.4

- [ ] Improve monitoring
- [ ] Improve Apalis Board
- [ ] Add job progress
- [ ] Add more sources

v 0.3

- [x] Standardize API (Storage, Worker, Data, Middleware, Context )
- [x] Introduce SQL
- [x] Implement layers for Sentry and Tracing.
- [x] Improve documentation
- [x] Organized modules and features.
- [x] Basic Web API Interface
- [x] Sql Examples
- [x] Sqlx migrations

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

[`tower::service`]: https://docs.rs/tower/latest/tower/trait.Service.html
[`tower`]: https://crates.io/crates/tower
[`actix`]: https://crates.io/crates/actix
[`tower-http`]: https://crates.io/crates/tower-http
[`actor`]: https://docs.rs/actix/0.13.0/actix/trait.Actor.html
