<h1 align="center">apalis</h1>
<div align="center">
 <strong>
   Simple and extensible multithreaded background task and messages processing library for Rust
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
  <a href="https://github.com/apalis-dev/apalis/actions">
    <img src="https://img.shields.io/github/actions/workflow/status/apalis-dev/apalis/ci.yaml?branch=main&style=flat-square"
      alt="CI" />
  </a>
</div>
<br/>

## Features

- **Simple and predictable task handling** - [Task handlers](https://docs.rs/apalis-core/1.0.0-beta.2/apalis_core/task_fn/guide/index.html) are just async functions with a macro-free API
- **Robust task execution** - Built-in support for retries, timeouts, and error handling
- **Multiple storage backends** - Support for Redis, PostgreSQL, SQLite, and in-memory storage
- **Advanced task management** - Task prioritization, scheduling, metadata, and result tracking
- **Scalable by design** - Distributed backends with configurable concurrency and multi-threaded execution
- **Familiar dependency injection** - Similar to popular frameworks like [`actix`] and [`axum`]
- **Runtime agnostic** - Works with tokio, async-std, and other async runtimes
- **Production ready** - Built-in monitoring, metrics, graceful shutdown, and comprehensive error reporting
- **Extensible middleware system** - Take full advantage of the [`tower`] ecosystem of services and utilities
- **Optional web interface** - Manage and monitor your tasks through a web UI

## Crate ecosystem

| Source             | Crate                                                                                                                         | Examples                                                                                                                                                                             |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `apalis-cron`      | <a href="https://docs.rs/apalis-cron"><img src="https://img.shields.io/crates/v/apalis-cron?style=flat-square"></a>           | <a href="https://github.com/apalis-dev/apalis-amqp/tree/main/examples/basic.rs"><img src="https://img.shields.io/badge/-basic-black?style=flat-square&logo=github"/></a>    |
| `apalis-redis`     | <a href="https://docs.rs/apalis-redis"><img src="https://img.shields.io/crates/v/apalis-redis?style=flat-square"></a>         | <a href="https://github.com/apalis-dev/apalis-redis"><img src="https://img.shields.io/badge/-basic-black?style=flat-square&logo=redis"/></a>                                |
| `apalis-sqlite`    | <a href="https://docs.rs/apalis-sqlite"><img src="https://img.shields.io/crates/v/apalis-sqlite?style=flat-square"></a>       | <a href="https://github.com/apalis-dev/apalis-sqlite"><img src="https://img.shields.io/badge/-basic-black?style=flat-square&logo=sqlite"/></a>                             |
| `apalis-postgres`  | <a href="https://docs.rs/apalis-postgres"><img src="https://img.shields.io/crates/v/apalis-postgres?style=flat-square"></a>   | <a href="https://github.com/apalis-dev/apalis-postgres"><img src="https://img.shields.io/badge/-basic-black?style=flat-square&logo=postgresql"/></a>                       |
| `apalis-mysql`     | <a href="https://docs.rs/apalis-mysql"><img src="https://img.shields.io/crates/v/apalis-mysql?style=flat-square"></a>         | <a href="https://github.com/apalis-dev/apalis-mysql"><img src="https://img.shields.io/badge/-basic-black?style=flat-square&logo=mysql"/></a>                                |
| `apalis-amqp`      | <a href="https://docs.rs/apalis-amqp"><img src="https://img.shields.io/crates/v/apalis-amqp?style=flat-square"></a>           | <a href="https://github.com/apalis-dev/apalis-amqp/tree/main/examples/basic.rs"><img src="https://img.shields.io/badge/-basic-black?style=flat-square&logo=github"/></a> |
| `apalis-core`      | <a href="https://docs.rs/apalis-core"><img src="https://img.shields.io/crates/v/apalis-core?style=flat-square"></a>           |                                                                                                                                                                                     |
| `apalis-workflow`  | <a href="https://docs.rs/apalis-workflow"><img src="https://img.shields.io/crates/v/apalis-workflow?style=flat-square"></a>   |                                                                                                                                                                                     |
| `apalis-board`     | <a href="https://docs.rs/apalis-board"><img src="https://img.shields.io/crates/v/apalis-board?style=flat-square"></a>         |                                                                                                                                                                                     |
| `apalis-board-api` | <a href="https://docs.rs/apalis-board-api"><img src="https://img.shields.io/crates/v/apalis-board-api?style=flat-square"></a> | <a href="https://github.com/apalis-dev/apalis-board/tree/master/examples/axum-email-service"><img src="https://img.shields.io/badge/axum-blue?style=flat-square&logo=github"/></a> <a href="https://github.com/apalis-dev/apalis-board/tree/master/examples/actix-ntfy-service"><img src="https://img.shields.io/badge/actix-red?style=flat-square&logo=github"/></a> |

## Getting Started

To get started, just add to Cargo.toml

```toml
[dependencies]
apalis = { version = "1.0.0-beta.2" }
# apalis-redis = { version = "1.0.0-alpha.1" } # Use redis/sqlite/postgres etc
```

## Usage

```rust,no_run
use apalis::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Email {
    to: String,
}

/// A function called for every task
async fn send_email(task: Email, data: Data<usize>) {
  // execute task
}


#[tokio::main]
async fn main() {
    let mut storage = MemoryStorage::new();
     storage
        .push(Email {
            to: "test@example.com".to_string(),
        })
        .await
        .expect("Could not add tasks");

    WorkerBuilder::new("email-worker")
      .backend(storage)
      .concurrency(2)
      .parallelize(tokio::spawn)
      .data(0usize)
      .build(send_email)
      .run()
      .await;
}
```

### Workflows

- `apalis` has first class support for sequential, dag and conditional workflows.

```rust,no_run
use apalis::prelude::*;
use apalis_workflow::*;
use std::time::Duration;
use apalis_core::backend::json::JsonStorage;

#[tokio::main]
async fn main() {
   let workflow = Workflow::new("odd-numbers-workflow")
       .delay_for(Duration::from_millis(1000))
       .and_then(|a: usize| async move { Ok::<_, BoxDynError>((0..a).collect::<Vec<_>>()) })
       .filter_map(|x| async move { if x % 2 != 0 { Some(x) } else { None } })
       .and_then(|a: Vec<usize>| async move {
           println!("Sum: {}", a.iter().sum::<usize>());
           Ok::<_, BoxDynError>(())
        });

   let mut in_memory = JsonStorage::new_temp().unwrap();

   in_memory.push_start(10).await.unwrap();

   let worker = WorkerBuilder::new("rango-tango")
       .backend(in_memory)
       .on_event(|ctx, ev| {
           println!("On Event = {:?}", ev);
       })
       .build(workflow);
   worker.run().await.unwrap();
}
```

For more functionality like `fold`, `filter_map` and other combinators checkout the [docs](https://docs.rs/apalis-workflow)

## Feature flags

- _full_ - All the available features
- _tracing_ (enabled by default) — Support Tracing 👀
- _sentry_ — Support for Sentry exception and performance monitoring
- _prometheus_ — Support Prometheus metrics
- _retry_ — Support direct retrying tasks
- _timeout_ — Support timeouts on tasks
- _limit_ — Support for concurrency and rate-limiting
- _filter_ — Support filtering tasks based on a predicate
- _catch-panic_ - Catch panics that occur during execution

## How apalis works

Here is a basic example of how the core parts integrate

```mermaid
sequenceDiagram
    participant App
    participant Worker
    participant Backend

    App->>+Backend: Add task to queue
    Backend-->>+Worker: Job data
    Worker->>+Backend: Update task status to 'Running'
    Worker->>+App: Started task
    loop task execution
        Worker-->>-App: Report task progress
    end
    Worker->>+Backend: Update task status to 'completed'
```

## Observability

With the [web UI](https://github.com/apalis-dev/apalis-board), you can manage your jobs through a simple interface. Check out this [working example](https://github.com/apalis-dev/apalis-board/blob/master/examples/axum-email-service) to see how it works.

![Workers Screenshot](https://github.com/apalis-dev/apalis-board/raw/master/screenshots/workers.png)

## Integrations

- [zino](https://crates.io/crates/zino-core): Next-generation framework for composable applications in Rust.
- [spring-rs](https://github.com/spring-rs/spring-rs): Application framework written in Rust, inspired by Java's SpringBoot

## Projects using apalis

- [Ryot](https://github.com/IgnisDa/ryot): A self hosted platform for tracking various facets of your life - media, fitness etc.
- [Hyprnote](https://github.com/fastrepl/hyprnote): Local-first AI Notepad for Private Meetings
- [oxy](https://github.com/oxy-hq/oxy): A framework for building SQL agents and automations for analytics.
- [OpenZeppelin Relayer](https://github.com/OpenZeppelin/openzeppelin-relayer): OpenZeppelin relayer service
- [Summarizer](https://github.com/akhildevelops/summarizer): Podcast summarizer
- [Universal Inbox](https://github.com/universal-inbox/universal-inbox): Universal Inbox is a solution that centralizes all your notifications and tasks in one place to create a unique inbox.
- [Hatsu](https://github.com/importantimport/hatsu): Self-hosted and fully-automated ActivityPub bridge for static sites.
- [Stamon](https://github.com/krivahtoo/stamon): A lightweight self-hosted status monitoring tool
- [Gq](https://github.com/jorgehermo9/gq): open-source filtering tool for JSON and YAML files

## External examples

- [Shuttle](https://github.com/shuttle-hq/shuttle-examples/tree/main/shuttle-cron): Using apalis-cron with [shuttle.rs](https://shuttle.rs)
- [Actix-Web](https://github.com/actix/examples/tree/master/background-jobs): Using apalis-redis with actix-web
- [Zino-Example](https://github.com/apalis-dev/zino-example): Using [zino](https://crates.io/crates/zino)

## Resources

- [Background job processing with rust using actix and redis](https://mureithi.me/blog/background-job-processing-with-rust-actix-redis)
- [Feasibility of implementing cronjob with Rust programming language](https://tpbabparn.medium.com/feasibility-of-implementing-cronjob-with-rust-programming-language-186eaed0a7d8)
- [How to schedule and run cron jobs in Rust using apalis](https://dev.to/njugunamureithi/how-to-schedule-and-run-cron-jobs-in-rust-5106)

## Thanks to

- [`tower`] - Tower is a library of modular and reusable components for building robust networking clients and servers.
- [`redis-rs`](https://github.com/mitsuhiko/redis-rs) - Redis library for rust
- [`sqlx`](https://github.com/launchbadge/sqlx) - The Rust SQL Toolkit
- [`cron`](https://docs.rs/cron/latest/cron/) - A cron expression parser and schedule explorer

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/apalis-dev/apalis/tags).

## Contributors

See also the list of [contributors](https://github.com/apalis-dev/apalis/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

[`tower::Service`]: https://docs.rs/tower/latest/tower/trait.Service.html
[`tower`]: https://crates.io/crates/tower
[`actix`]: https://crates.io/crates/actix
[`axum`]: https://crates.io/crates/axum
