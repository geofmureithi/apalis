#![crate_name = "apalis_cron"]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! # apalis-cron
//!
//! A cron-like job scheduling library for `apalis` that is simple yet extensible.
//!
//! `apalis-cron` is built on top of `apalis` and integrates seamlessly with the `apalis` ecosystem.
//! This means you can leverage the full power of workers and middleware, including:
//!
//! - **Tracing**: For observing the execution of your cron jobs.
//! - **Retries**: To handle transient failures with configurable backoff strategies.
//! - **Concurrency**: To control how many instances of a job can run simultaneously.
//! - **Load-shedding**: To prevent your system from being overloaded.
//!
//! ## Features
//!
//! - **Cron-based Scheduling**: Use standard cron expressions to define your job schedules.
//! - **Timezone Support**: Schedule jobs in any timezone.
//! - **Persistence**: Persist cron jobs to a storage backend (e.g., Postgres, MySQL, SQLite) to ensure they are not lost on restart and can be distributed across multiple workers.
//! - **Extensibility**: Easily add custom middleware and services.
//!
//! ## Getting Started
//!
//! To use `apalis-cron`, you'll need to add it to your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! apalis-cron = "1"
//! apalis = { version = "1", features = ["limit"] }
//! tokio = { version = "1", features = ["full"] }
//! chrono = "0.4"
//! ```
//!
//! ## Example
//!
//! Here's a basic example of how to schedule a cron job that runs every day:
//!
//! ```rust,no_run
//! use apalis::{prelude::*, layers::retry::RetryPolicy};
//! use std::str::FromStr;
//! use apalis_cron::{CronStream, Schedule, CronContext};
//! use chrono::Local;
//!
//! // The handler for the cron tick
//! async fn handle_reminder(tick: Tick, data: Data<usize>) {
//!     println!(
//!         "Good morning! It's time for your daily reminder at {}. Data: {}",
//!         tick.get_timestamp(),
//!         data.0,
//!     );
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     // The cron schedule for the job. This runs every day at midnight.
//!     let schedule = Schedule::from_str("@daily").unwrap();
//!
//!     // Create a worker that executes the job.
//!     let worker = WorkerBuilder::new("daily-reminder")
//!         .retry(RetryPolicy::retries(5))
//!         .data(42usize)
//!         .backend(CronStream::new(schedule))
//!         .build_fn(handle_reminder);
//!
//!     worker.run().await;
//! }
//! ```
//!
//! ## Timezones
//!
//! By default, `apalis-cron` uses `Utc`. However, you can specify a different timezone.
//!
//! ```rust,no_run
//! # use apalis::prelude::*;
//! # use std::str::FromStr;
//! # use apalis_cron::{CronStream, Schedule, CronContext};
//! # use chrono::Local;
//! async fn handle_reminder(tick: Tick<Local>) {
//!     println!("Reminder for timezone: {}", tick.get_timestamp());
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let schedule = Schedule::from_str("0 0 * * * *").unwrap(); // Every hour
//!
//!     let worker = WorkerBuilder::new("new-york-reminder")
//!         .backend(CronStream::new_with_timezone(schedule, Local))
//!         .build_fn(handle_reminder);
//!
//!     worker.run().await;
//! }
//! ```
//!
//! ## Persisting Cron Jobs
//!
//! In a production environment, you might want to persist cron jobs for several reasons:
//!
//! - **Distribution**: Distribute cron jobs across multiple servers for high availability.
//! - **Durability**: Ensure that jobs are not lost if the application restarts.
//! - **Observability**: Store the results and history of cron jobs for auditing or debugging.
//!
//! `apalis-cron` makes this easy by allowing you to pipe cron events to any backend that implements `TaskSink`.
//!
//! ```rust,no_run
//! # use apalis::{prelude::*};
//! # use apalis_sql::sqlite::{SqliteStorage, SqlitePool};
//! # use std::str::FromStr;
//! # use apalis_cron::{CronStream, Schedule, CronContext};
//! # use chrono::Local;
//! async fn handle_reminder(tick: Tick<Local>) {
//!     // Your job logic here
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let schedule = Schedule::from_str("@daily").unwrap();
//!     let cron_stream = CronStream::new(schedule);
//!
//!     // Create a storage for our cron jobs (e.g., SQLite)
//!     let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
//!     SqliteStorage::setup(&pool)
//!         .await
//!         .expect("unable to run migrations for sqlite");
//!     let storage = SqliteStorage::new(pool);
//!
//!     // Pipe the cron stream to the storage backend
//!     let backend = cron_stream.pipe_to(storage);
//!
//!     let worker = WorkerBuilder::new("persistent-reminder")
//!         .backend(backend)
//!         .build_fn(handle_reminder);
//!
//!     worker.run().await;
//! }
//! ```
//! # Feature flags
#![cfg_attr(
    feature = "docsrs",
    cfg_attr(doc, doc = ::document_features::document_features!())
)]
mod backend;
mod context;
mod error;
mod schedule;
mod tick;
mod timezone;

const FORMAT: &str = "%Y-%m-%d %H:%M:%S";

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use apalis_core::{
        backend::{json::JsonStorage, pipe::PipeExt},
        error::BoxDynError,
        task::task_id::TaskId,
        worker::{builder::WorkerBuilder, event::Event, ext::event_listener::EventListenerExt},
    };
    use chrono::Utc;
    use cron::Schedule;
    use tower::{limit::ConcurrencyLimitLayer, load_shed::LoadShedLayer};

    use crate::{backend::CronStream, context::CronContext, tick::Tick};

    #[tokio::test]
    async fn basic_worker() {
        let schedule = Schedule::from_str("1/1 * * * * *").unwrap();
        let stream = CronStream::new_with_timezone(schedule, Utc);

        async fn send_reminder(
            tick: Tick<Utc>,
            meta: CronContext<Schedule>,
        ) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            println!(
                "Running cronjob for timestamp: {} with meta: {:?}",
                tick.get_timestamp(),
                meta
            );
            Err("Failed".into())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(stream)
            .on_event(move |ctx, ev| {
                println!("{:?}", ev);
                let ctx = ctx.clone();
                if matches!(ev, Event::Start) {
                    tokio::spawn(async move {
                        if ctx.is_running() {
                            tokio::time::sleep(Duration::from_millis(900)).await;
                            ctx.stop().unwrap();
                        }
                    });
                }
            })
            .build(send_reminder);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn load_shedding_worker() {
        // We are generating a new cron job every second
        let schedule = Schedule::from_str("1/1 * * * * *").unwrap();
        let stream = CronStream::new_with_timezone(schedule, Utc);

        // But a single job can take longer than a second to complete
        async fn send_reminder(
            tick: Tick<Utc>,
            ctx: CronContext<Schedule>,
        ) -> Result<(), BoxDynError> {
            println!(
                "Running cronjob for timestamp: {} with ctx: {:?}",
                tick.get_timestamp(),
                ctx.schedule().unwrap().to_string()
            );
            tokio::time::sleep(Duration::from_secs(2)).await;
            Err("Failed".into())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(stream)
            // Drop requests if there is existing one
            .layer(LoadShedLayer::new())
            // Limit concurrent requests
            .layer(ConcurrencyLimitLayer::new(1))
            .on_event(move |ctx, ev| {
                println!("{:?}", ev);
                let ctx = ctx.clone();
                if matches!(ev, Event::Start) {
                    tokio::spawn(async move {
                        if ctx.is_running() {
                            tokio::time::sleep(Duration::from_millis(3000)).await;
                            ctx.stop().unwrap();
                        }
                    });
                }
            })
            .build(send_reminder);
        // This might log
        //
        // $ Running cronjob for timestamp: 2025-09-04 23:28:08 UTC with ctx: "1/1 * * * * *"
        // $ Error("Failed")
        // $ Error(Overloaded)
        // $ Running cronjob for timestamp: 2025-09-04 23:28:10 UTC with ctx: "1/1 * * * * *"
        worker.run().await.unwrap();
    }

    #[cfg(feature = "serde")]
    #[tokio::test]
    async fn piped_worker() {
        let schedule = Schedule::from_str("1/1 * * * * *").unwrap();
        let stream = CronStream::new(schedule);
        let in_memory = JsonStorage::new_temp().unwrap();

        let backend = stream.pipe_to(in_memory);

        async fn send_reminder(job: Tick, id: TaskId) -> Result<(), BoxDynError> {
            println!("Running cronjob for timestamp: {:?} with id {}", job, id);
            tokio::time::sleep(Duration::from_secs(1)).await;
            Err("All failing".into())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(move |ctx, ev| {
                println!("{:?}", ev);
                let ctx = ctx.clone();
                if matches!(ev, Event::Error(_)) {
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        ctx.stop().unwrap();
                    });
                }
            })
            .build(send_reminder);
        worker.run().await.unwrap();
    }
}
