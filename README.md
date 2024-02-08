<h1 align="center">apalis</h1>
<div align="center">
 <strong>
   Simple, extensible multithreaded background job and messages processing library for Rust
 </strong>
</div>

<br />

<div align="center">
  <!-- Crates version -->
  <a href="https://crates.io/crates/apalis">
    <img src="https://img.shields.io/crates/v/apalis.svg?style=flat-square"
    alt="Crates.io version" />
  </a>
  <!-- Downloads -->
  <a href="https://crates.io/crates/apalis">
    <img src="https://img.shields.io/crates/d/apalis.svg?style=flat-square"
      alt="Download" />
  </a>
  <!-- docs.rs docs -->
  <a href="https://docs.rs/apalis">
    <img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square"
      alt="docs.rs docs" />
  </a>
  <a href="https://github.com/geofmureithi/apalis/actions">
    <img src="https://img.shields.io/github/actions/workflow/status/geofmureithi/apalis/ci.yaml?branch=master&style=flat-square"
      alt="CI" />
  </a>
</div>
<br/>

## Features

- Simple and predictable job handling model.
- Jobs handlers with a macro free API.
- Take full advantage of the [`tower`] ecosystem of
  middleware, services, and utilities.
- Runtime agnostic - Use tokio, smol etc.
- Optional Web interface to help you manage your jobs.

apalis job processing is powered by [`tower::Service`] which means you have access to the [`tower`] middleware.

apalis has support for:

| Source       | Crate                                                                                                                 | Example                                                                                                                                                                                  |
| ------------ | --------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Cron Jobs    | <a href="https://docs.rs/apalis-cron"><img src="https://img.shields.io/crates/v/apalis-cron?style=flat-square"></a>   | <a href="https://github.com/geofmureithi/apalis/tree/master/examples/async-std-runtime"><img src="https://img.shields.io/badge/-basic_example-black?style=flat-square&logo=github"/></a> |
| Redis        | <a href="https://docs.rs/apalis-redis"><img src="https://img.shields.io/crates/v/apalis-redis?style=flat-square"></a> | <a href="https://github.com/geofmureithi/apalis/tree/master/examples/redis"><img src="https://img.shields.io/badge/-basic_example-black?style=flat-square&logo=redis"/></a>              |
| Sqlite       | <a href="https://docs.rs/apalis-sql"><img src="https://img.shields.io/crates/v/apalis-sql?style=flat-square"></a>     | <a href="https://github.com/geofmureithi/apalis/tree/master/examples/sqlite"><img src="https://img.shields.io/badge/-sqlite_example-black?style=flat-square&logo=sqlite"/></a>           |
| Postgres     | <a href="https://docs.rs/apalis-sql"><img src="https://img.shields.io/crates/v/apalis-sql?style=flat-square"></a>     | <a href="https://github.com/geofmureithi/apalis/tree/master/examples/postgres"><img src="https://img.shields.io/badge/-postgres_example-black?style=flat-square&logo=postgres"/></a>     |
| MySQL        | <a href="https://docs.rs/apalis-sql"><img src="https://img.shields.io/crates/v/apalis-sql?style=flat-square"></a>     | <a href="https://github.com/geofmureithi/apalis/tree/master/examples/mysql"><img src="https://img.shields.io/badge/-mysql_example-black?style=flat-square&logo=mysql"/></a>              |
| Amqp         | <a href="https://docs.rs/apalis-amqp"><img src="https://img.shields.io/crates/v/apalis-amqp?style=flat-square"></a>   | <a href="https://github.com/geofmureithi/apalis-amqp/tree/master/examples/basic.rs"><img src="https://img.shields.io/badge/-rabbitmq_example-black?style=flat-square&logo=github"/></a>  |
| From Scratch | <a href="https://docs.rs/apalis-core"><img src="https://img.shields.io/crates/v/apalis-core?style=flat-square"></a>   |                                                                                                                                                                                          |

## Getting Started

To get started, just add to Cargo.toml

```toml
[dependencies]
apalis = { version = "0.4", features = ["redis"] }
```

## Usage

````rust
use apalis::prelude::*;
use apalis::redis::RedisStorage;
use serde::{Deserialize, Serialize};
use anyhow::Result;

#[derive(Debug, Deserialize, Serialize)]
struct Email {
    to: String,
}

impl Job for Email {
    const NAME: &'static str = "apalis::Email";
}

/// A function that will be converted into a service.
/// ```
async fn send_email(job: Email, data: Data<usize>) -> Result<(), Error> {
  /// execute job
  Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();
    let redis_url = std::env::var("REDIS_URL").expect("Missing env variable REDIS_URL");
    let storage = RedisStorage::new(redis).await?;
    Monitor::new()
        .register_with_count(2, {
            WorkerBuilder::new(format!("email-worker"))
                .with_storage(storage)
                .build_fn(send_email)
        })
        .run()
        .await
}

````

Then

```rust
//This can be in another part of the program or another application eg a http server
async fn produce_route_jobs(storage: &RedisStorage<Email>) -> Result<()> {
    let mut storage = storage.clone();
    storage
        .push(Email {
            to: "test@example.com".to_string(),
        })
        .await?;
}

```

## Feature flags

- _tracing_ (enabled by default) — Support Tracing 👀
- _redis_ — Include redis storage
- _postgres_ — Include Postgres storage
- _sqlite_ — Include SQlite storage
- _mysql_ — Include MySql storage
- _cron_ — Include cron job processing
- _sentry_ — Support for Sentry exception and performance monitoring
- _prometheus_ — Support Prometheus metrics
- _retry_ — Support direct retrying jobs
- _timeout_ — Support timeouts on jobs
- _limit_ — 💪 Limit the amount of jobs
- _filter_ — Support filtering jobs based on a predicate
- _extensions_ — Add a global extensions to jobs

## Storage Comparison

Since we provide a few storage solutions, here is a table comparing them:

| Feature         | Redis | Sqlite | Postgres | Sled | Mysql | Mongo | Cron |
| :-------------- | :---: | :----: | :------: | :--: | :---: | :---: | :--: |
| Scheduled jobs  |   ✓   |   ✓    |    ✓     |  x   |   ✓   |   x   |  ✓   |
| Retry jobs      |   ✓   |   ✓    |    ✓     |  x   |   ✓   |   x   |  ✓   |
| Persistence     |   ✓   |   ✓    |    ✓     |  x   |   ✓   |   x   | BYO  |
| Rerun Dead jobs |   ✓   |   ✓    |    ✓     |  x   |   ✓   |   x   |  x   |

## How apalis works

Here is a basic example of how the core parts integrate

```mermaid
sequenceDiagram
    participant App
    participant Worker
    participant Backend

    App->>+Backend: Add job to queue
    Backend-->>+Worker: Job data
    Worker->>+Backend: Update job status to 'Running'
    Worker->>+App: Started job
    loop job execution
        Worker-->>-App: Report job progress
    end
    Worker->>+Backend: Update job status to 'completed'
```

### Web UI

If you are running [apalis Board](https://github.com/geofmureithi/apalis-board), you can easily manage your jobs. See a working [rest API example here](https://github.com/geofmureithi/apalis/tree/master/examples/rest-api)

<img src="https://github.com/geofmureithi/apalis-board/raw/master/screenshots/workers.png" width="100%">

## Thanks to

- [`tower`] - Tower is a library of modular and reusable components for building robust networking clients and servers.
- [redis-rs](https://github.com/mitsuhiko/redis-rs) - Redis library for rust
- [sqlx](https://github.com/launchbadge/sqlx) - The Rust SQL Toolkit

## Roadmap

v 0.5

- [x] Refactor the crates structure
- [x] Mocking utilities
- [ ] Support for SurrealDB and Mongo
- [ ] Lock free for Postgres
- [x] Add more utility layers
- [x] Use extractors in job fn structure
- [x] Polish up documentation
- [ ] Improve and standardize apalis Board
- [ ] Benchmarks

v 0.4

- [x] Move from actor based to layer based processing
- [x] Graceful Shutdown
- [x] Allow other types of executors apart from Tokio
- [x] Mock/Test Worker
- [x] Improve monitoring
- [x] Add job progress via layer
- [x] Add more sources

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

## Resources

- [Background job processing with rust using actix and redis](https://mureithi.me/blog/background-job-processing-with-rust-actix-redis)

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/geofmureithi/apalis/tags).

## Authors

- **Njuguna Mureithi** - _Initial work_ - [Njuguna Mureithi](https://github.com/geofmureithi)

See also the list of [contributors](https://github.com/geofmureithi/apalis/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

[`tower::Service`]: https://docs.rs/tower/latest/tower/trait.Service.html
[`tower`]: https://crates.io/crates/tower
[`actix`]: https://crates.io/crates/actix
[`tower-http`]: https://crates.io/crates/tower-http
[`actor`]: https://docs.rs/actix/0.13.0/actix/trait.Actor.html
