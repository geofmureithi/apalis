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
//! // The job to be executed
//! #[derive(Default, Debug, Clone)]
//! struct Reminder;
//!
//! // The handler for the job
//! async fn handle_reminder(job: CronContext, data: Data<usize>) {
//!     println!(
//!         "Good morning! It's time for your daily reminder at {}. Data: {}",
//!         job.get_timestamp(),
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
//!         .retry(RetryPolicy::retries(5)) // Add middleware
//!         .data(42usize) // You can add data to the worker context
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
//! use apalis::prelude::*;
//! use std::str::FromStr;
//! use apalis_cron::{CronStream, Schedule, CronContext};
//! use chrono::Local;
//!
//! async fn handle_reminder(job: CronContext<Local>) {
//!     println!("Reminder for timezone: {}", job.get_timestamp());
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
//! `apalis-cron` makes this easy by allowing you to pipe cron events to any `apalis` storage backend.
//!
//! ```rust,no_run
//! use apalis::{prelude::*};
//! use apalis_sql::sqlite::{SqliteStorage, SqlitePool};
//! use std::str::FromStr;
//! use apalis_cron::{CronStream, Schedule, CronContext};
//! use chrono::Local;
//!
//! async fn handle_reminder(job: CronContext<Local>) {
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

use apalis_core::backend::pipe::{Pipe, PipeExt};
use apalis_core::backend::{Backend, TaskStream};
use apalis_core::error::BoxDynError;
use apalis_core::task::from_request::FromRequest;
use apalis_core::task::attempt::Attempt;
use apalis_core::task::builder::TaskBuilder;
use apalis_core::task::data::MissingDataError;
use apalis_core::task::extensions::Extensions;
use apalis_core::task::task_id::{TaskId, Ulid};
use apalis_core::task::{Parts, Task};
use apalis_core::timer::Delay;
use apalis_core::worker::context::WorkerContext;
use chrono::{DateTime, Days, NaiveDateTime, Offset, OutOfRangeError, TimeDelta, TimeZone, Utc};
pub use cron::Schedule;
use futures::stream::{self, BoxStream};
use futures::{Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::convert::Infallible;
use std::fmt::{self, Debug, Display};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};
use tower::layer::util::Identity;

/// Represents a stream from a cron schedule with a timezone
#[doc = "# Feature Support\n"]
#[derive(Debug)]
pub struct CronStream<Tz: TimeZone> {
    schedule: Schedule,
    timezone: Tz,
    next_tick: Option<DateTime<Tz>>,
    delay: Option<Delay>,
}

impl CronStream<Utc> {
    /// Build a new cron stream from a schedule using the UTC timezone
    pub fn new(schedule: Schedule) -> Self {
        Self::new_with_timezone(schedule, Utc)
    }
}

impl<Tz> CronStream<Tz>
where
    Tz: TimeZone + Send + Sync + 'static,
{
    /// Build a new cron stream from a schedule and timezone
    pub fn new_with_timezone(schedule: Schedule, timezone: Tz) -> Self {
        Self {
            schedule,
            timezone,
            next_tick: None,
            delay: None,
        }
    }
}

impl<Tz: TimeZone + Unpin> Stream for CronStream<Tz>
where
    Tz::Offset: Unpin,
{
    type Item = Result<Tick<Tz>, CronStreamError<Tz>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        loop {
            match &mut this.next_tick {
                Some(next) => {
                    // If we haven't set the delay yet, set it now.
                    if this.delay.is_none() {
                        let now = Utc::now();
                        let td = next.clone().signed_duration_since(now);
                        let duration = match td.to_std() {
                            Ok(d) => d,
                            Err(e) => {
                                return Poll::Ready(Some(Err(CronStreamError::OutOfRangeError {
                                    inner: e,
                                    tick: next.clone(),
                                })))
                            }
                        };
                        this.delay = Some(Delay::new(duration));
                    }

                    // Poll the delay future
                    match Pin::new(this.delay.as_mut().unwrap()).poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(()) => {
                            let fired = next.clone();

                            // Update next_tick and delay.
                            let mut upcoming = self.schedule.upcoming(self.timezone.clone());
                            self.next_tick = upcoming.find(|dt| *dt > fired);
                            self.delay = None;
                            return Poll::Ready(Some(Ok(Tick { timestamp: fired })));
                        }
                    }
                }
                None => {
                    let mut upcoming = this.schedule.upcoming(this.timezone.clone());
                    let next_tick = upcoming.next();
                    match next_tick {
                        Some(next) => this.next_tick = Some(next),
                        None => return Poll::Ready(None),
                    }
                }
            }
        }
    }
}

impl<Ctx, Tz, B> PipeExt<B, Tick<Tz>, Ctx> for CronStream<Tz>
where
    Tz: TimeZone + Send + Sync + 'static,
    Tz::Offset: Send + Sync,
    B: Backend<Tick<Tz>>,
    B::Error: Into<BoxDynError> + Send + Sync + 'static,
{
    fn pipe_to(self, backend: B) -> Pipe<Self, B, Tick<Tz>, Ctx> {
        Pipe::new(self, backend)
    }
}

impl<Tz: TimeZone> Default for Tick<Tz>
where
    DateTime<Tz>: Default,
{
    fn default() -> Self {
        Self {
            timestamp: Default::default(),
        }
    }
}

impl<Tz: TimeZone> Tick<Tz> {
    /// Create a new context provided a timestamp
    pub fn new(timestamp: DateTime<Tz>) -> Self {
        Self { timestamp }
    }

    /// Get the inner timestamp
    pub fn get_timestamp(&self) -> &DateTime<Tz> {
        &self.timestamp
    }
}

impl<Tz: Unpin> Backend<Tick<Tz>> for CronStream<Tz>
where
    Tz: TimeZone + Send + Sync + 'static,
    Tz::Offset: Send + Sync + Unpin + Display,
{
    type Ctx = CronContext;
    type Codec = ();
    type Error = CronStreamError<Tz>;
    type Stream = TaskStream<Task<Tick<Tz>, CronContext, Ulid>, CronStreamError<Tz>>;

    type Layer = Identity;

    type IdType = Ulid;

    type Beat = BoxStream<'static, Result<(), Self::Error>>;

    fn heartbeat(&self, _: &WorkerContext) -> Self::Beat {
        stream::once(async { Ok(()) }).boxed()
    }
    fn middleware(&self) -> Self::Layer {
        Identity::new()
    }

    fn poll(self, _: &WorkerContext) -> Self::Stream {
        let meta = CronContext {
            schedule: Arc::new(self.schedule.clone()),
        };
        let stream = self.and_then(move |tick| {
            let meta = meta.clone();
            async move {
                let timestamp: SystemTime = tick.get_timestamp().clone().into();
                let task_id = Ulid::from_datetime(timestamp);
                let task = TaskBuilder::new_with_metadata(tick, meta.clone())
                    .with_task_id(TaskId::new(task_id))
                    .build();

                Ok(Some(task))
            }
        });
        stream.boxed()
    }
}

/// Represents an error emitted by `CronStream` polling
pub enum CronStreamError<Tz: TimeZone> {
    /// The cron stream might not always be polled consistently, such as when the worker is blocked.
    /// If polling is delayed, some ticks may be skipped. When this occurs, an out-of-range error is triggered
    /// because the missed tick is now in the past.
    OutOfRangeError {
        /// The inner error
        inner: OutOfRangeError,
        /// The missed tick
        tick: DateTime<Tz>,
    },
}

impl<Tz: TimeZone> fmt::Display for CronStreamError<Tz>
where
    Tz::Offset: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CronStreamError::OutOfRangeError { inner, tick } => {
                write!(
                    f,
                    "Cron tick {} is out of range: {}",
                    tick.format(FORMAT),
                    inner
                )
            }
        }
    }
}

impl<Tz: TimeZone> std::error::Error for CronStreamError<Tz>
where
    Tz::Offset: Display,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CronStreamError::OutOfRangeError { inner, .. } => Some(inner),
        }
    }
}

impl<Tz: TimeZone> fmt::Debug for CronStreamError<Tz> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CronStreamError::OutOfRangeError { inner, tick } => f
                .debug_struct("OutOfRangeError")
                .field("tick", tick)
                .field("inner", inner)
                .finish(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Tick<Tz: TimeZone = Utc> {
    pub timestamp: DateTime<Tz>,
}

#[derive(Debug, Clone)]
pub struct CronContext {
    schedule: Arc<Schedule>,
}

const FORMAT: &str = "%Y-%m-%d %H:%M:%S";

impl<Tz> Serialize for Tick<Tz>
where
    Tz: TimeZone,
    Tz::Offset: std::fmt::Display,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = self.timestamp.format(FORMAT).to_string();
        serializer.serialize_str(&s)
    }
}

impl<'de, Tz> Deserialize<'de> for Tick<Tz>
where
    Tz: TimeZone + TimeZoneExt,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let naive = NaiveDateTime::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)?;
        let datetime =
            Tz::from_utc_datetime(&Tz::from_offset(&Tz::utc_offset_from_naive(&naive)), &naive);
        Ok(Tick {
            timestamp: datetime,
        })
    }
}

/// Helper trait to synthesize a timezone from offset
pub trait TimeZoneExt: TimeZone {
    fn utc_offset_from_naive(naive: &NaiveDateTime) -> Self::Offset;
}

impl TimeZoneExt for chrono::Utc {
    fn utc_offset_from_naive(_: &NaiveDateTime) -> Self::Offset {
        chrono::Utc
    }
}

impl TimeZoneExt for chrono::Local {
    fn utc_offset_from_naive(naive: &NaiveDateTime) -> Self::Offset {
        chrono::Local.offset_from_utc_datetime(naive)
    }
}

impl<Args: Sync, IdType: Sync> FromRequest<Task<Args, CronContext, IdType>> for CronContext {
    type Error = Infallible;
    async fn from_request(req: &Task<Args, CronContext, IdType>) -> Result<Self, Self::Error> {
        Ok(req.ctx.ctx.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use chrono::Local;

    use apalis_core::{
        backend::memory::MemoryStorage,
        worker::{builder::WorkerBuilder, event::Event, ext::event_listener::EventListenerExt},
    };

    use super::*;

    #[tokio::test]
    async fn basic_worker() {
        let schedule = Schedule::from_str("1/1 * * * * *").unwrap();
        let stream = CronStream::new_with_timezone(schedule, Utc);

        async fn send_reminder(tick: Tick<Utc>, meta: CronContext) -> Result<(), BoxDynError> {
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
    async fn piped_worker() {
        let schedule = Schedule::from_str("1/1 * * * * *").unwrap();
        let stream = CronStream::new(schedule);
        let in_memory = MemoryStorage::new();

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
