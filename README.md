# Apalis [![Build Status](https://travis-ci.org/geofmureithi/apalis.svg?branch=master)](https://travis-ci.org/geofmureithi/apalis)

Apalis is a simple, extensible multithreaded background job processing library for Rust.

## Features

- Simple and predictable job handling model.
- Jobs handlers with a macro free API.
- Take full advantage of the [`tower`] ecosystem of
  middleware, services, and utilities.
- Takes full of the [`actix`] actors with each queue being an [`Actor`].

Apalis job processing is powered by [`tower::Service`] which means you have access to the [`tower`] and [`tower-http`] middleware.

Apalis has support for Redis, SQlite, PostgresSQL and MySQL.

## Getting Started

To get started, just add to Cargo.toml

```toml
[dependencies]
apalis = { version = "0.3", features = ["redis"] }
```

### Prerequisites

A running redis server is required.
You can quickly use docker:

```bash
docker run --name some-redis -d redis
```

## Usage

```rust
use apalis::{redis::RedisStorage, JobError, JobRequest, JobResult, QueueBuilder, Storage, Worker};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct Email {
    to: String,
}

async fn email_service(job: JobRequest<Email>) -> Result<JobResult, JobError> {
    Ok(JobResult::Success)
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();
    let redis = std::env::var("REDIS_URL").expect("Missing env variable REDIS_URL");
    let storage = RedisStorage::new(redis).await.unwrap();
    Worker::new()
        .register_with_count(2, move || {
            QueueBuilder::new(storage.clone())
                .build_fn(email_service)
                .start()
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

## Storage Comparison

Since we provide a few storage solutions, here is a table comparing them:

| Feature        | Redis | Sqlite | Postgres | Sled | Mysql | Mongo |
| :------------- | :---: | :----: | :------: | :--: | ----- | ----- |
| Priorities     |       |        |          |      |       |       |
| Scheduled jobs |   ✓   |   ✓    |    ✓     |  ✓   | ✓     | -     |
| Retryable jobs |   ✓   |   ✓    |    ✓     |      | ✓     | -     |
| Persistence    |   ✓   |   ✓    |    ✓     |  ✓   | ✓     | -     |

## Built On

- [`actix`] - Actor framework for Rust
- [redis-rs](https://github.com/mitsuhiko/redis-rs) - Redis library for rust
- [sqlx](https://github.com/launchbadge/sqlx) - The Rust SQL Toolkit

## Roadmap

v 0.3

- [x] Standardize API (Storage, Worker, Data, Middleware, Context )
- [x] Introduce SQL
- [ ] Implement layers for sentry and tracing.
- [ ] Improve documentation
- [ ] Organized modules and features.
- [ ] Basic Web API Interface
- [x] Sql Examples

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
