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
//! `apalis-cron` is a flexible and extensible Rust library for scheduling and running cron jobs within the `apalis` ecosystem. It enables developers to define jobs using cron expressions, natural language routines, or custom schedules, and provides robust features for persistence, retries, concurrency, and observability.
//!
//! ## Features
//!
//! - **Cron-based Scheduling**: Use standard cron expressions to define your job schedules.
//! - **Timezone Support**: Schedule jobs in any timezone.
//! - **Persistence**: Persist cron jobs to a storage backend (e.g., Postgres, MySQL, SQLite) to ensure they are not lost on restart and can be distributed across multiple workers.
//! - **Extensibility**: Easily add custom middleware and services.
//!
//! ### Middleware support
//!
//! `apalis-cron` is built on top of `apalis` and `tower`.
//! This means you can leverage the full power of workers and middleware, including:
//!
//! - **Tracing**: For observing the execution of your cron jobs.
//! - **Retries**: To handle transient failures with configurable backoff strategies.
//! - **Concurrency**: To control how many instances of a job can run simultaneously.
//! - **Load-shedding**: To prevent your system from being overloaded by slow cron jobs.
//!
//! ## Examples
//!
//! ### Using `cron` crate
//!
//! ```rust,no_run
//! use std::str::FromStr;
//! # use apalis_cron::Tick;
//! # use apalis_cron::{CronStream};
//! # use chrono::{DateTime, Utc};
//! # use apalis_core::worker::builder::WorkerBuilder;
//! use cron::Schedule;
//!
//! async fn handle_tick(tick: Tick) {
//!     // Do something with the current tick
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let schedule = Schedule::from_str("@daily").unwrap();
//!
//!     let worker = WorkerBuilder::new("morning-cereal")
//!         .backend(CronStream::new(schedule))
//!         .build(handle_tick);
//!
//!     worker.run().await;
//! }
//! ```
//!
//! ### Using the builder pattern
//!
//! ```rust,no_run
//! # use apalis_cron::Tick;
//! # use apalis_cron::{CronStream};
//! # use chrono::{DateTime, Utc};
//! # use apalis_core::worker::builder::WorkerBuilder;
//! use apalis_cron::builder::schedule;
//!
//! # async fn handle_tick(tick: Tick) {}
//! 
//! #[tokio::main]
//! async fn main() {
//!     let schedule = schedule().each().day().build();
//!
//!     let worker = WorkerBuilder::new("morning-cereal")
//!         .backend(CronStream::new(schedule))
//!         .build(handle_tick);
//!
//!     worker.run().await;
//! }
//! ```
//!
//! ### Using the `english-to-cron` crate
//!
//! ```rust,no_run
//! # use apalis_cron::Tick;
//! # use apalis_cron::{CronStream};
//! # use chrono::{DateTime, Utc};
//! # use apalis_core::worker::builder::WorkerBuilder;
//! use std::str::FromStr;
//! use apalis_cron::english::EnglishRoutine;
//!
//! # async fn handle_tick(tick: Tick) {}
//! 
//! #[tokio::main]
//! async fn main() {
//!     let schedule = EnglishRoutine::from_str("every day").unwrap();
//!
//!     let worker = WorkerBuilder::new("morning-cereal")
//!         .backend(CronStream::new(schedule))
//!         .build(handle_tick);
//!
//!     worker.run().await;
//! }
//! ```
//!
//! ## Persistence
//!
//! Sometimes we may want to persist cron jobs for several reasons:
//!
//! - Distribute cronjobs between multiple servers
//! - Store the results of the cronjob
//! - Prevent task skipping in the case of a restart
//!
//! ```rust,ignore
//! #[tokio::main]
//! async fn main() {
//!     let schedule = Schedule::from_str("@daily").unwrap();
//!     let cron_stream = CronStream::new(schedule);
//!
//!     // Lets create a storage for our cron jobs
//!     let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
//!     SqliteStorage::setup(&pool)
//!         .await
//!         .expect("unable to run migrations for sqlite");
//!     let sqlite = SqliteStorage::new(pool);
//!
//!     let backend = cron_stream.pipe_to(sqlite);
//!
//!     let worker = WorkerBuilder::new("morning-cereal")
//!         .backend(backend)
//!         .build(handle_tick);
//!
//!     worker.run().await;
//! }
//! ```
//!
//! ## Implementing `Schedule`
//!
//! You can customize the way ticks are provided by implementing your own `Schedule`;
//!
//! ```rust,no_run
//! # use apalis_cron::Schedule;
//! # use chrono::Local;
//! # use chrono::DateTime;
//! # use chrono::NaiveTime;
//! # use apalis_cron::CronStream;
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_cron::Tick;
//! 
//! /// Daily routine at 8am
//! #[derive(Clone)]
//! struct MyDailyRoutine;
//!
//! impl Schedule<Local> for MyDailyRoutine {
//!     fn next_tick(&mut self, timezone: &Local) -> Option<DateTime<Local>> {
//!         let now = Local::now();
//!         // Add 1 day to get tomorrow
//!         let tomorrow = now.date_naive() + chrono::Duration::days(1);
//!
//!         // Define 8:00 AM as a NaiveTime
//!         let eight_am = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
//!
//!         // Combine tomorrow's date with 8:00 AM in local time zone
//!         let tomorrow_eight_am = tomorrow.and_time(eight_am).and_local_timezone(Local).unwrap();
//!
//!
//!         Some(tomorrow_eight_am)
//!     }
//! }
//!
//! async fn handle_tick(tick: Tick<Local>) {
//!     // Handle the tick event
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let cron_stream = CronStream::new_with_timezone(MyDailyRoutine, Local);
//!     let worker = WorkerBuilder::new("morning-cereal")
//!         .backend(cron_stream)
//!         .build(handle_tick);
//!
//!     worker.run().await;
//! }
//! ```
//!
//! ## Feature flags
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

pub use {backend::*, context::*, error::*, schedule::*, tick::*, timezone::*};

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
