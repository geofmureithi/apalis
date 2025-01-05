#![crate_name = "apalis_cron"]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! # apalis-cron
//! A simple yet extensible library for cron-like job scheduling for rust.
//! Since `apalis-cron` is build on top of `apalis` which supports tower middleware, you should be able to easily
//! add middleware such as tracing, retries, load-shed, concurrency etc.
//!
//! ## Example
//!
//! ```rust,no_run
//! use apalis::{prelude::*, layers::retry::RetryPolicy};
//! use std::str::FromStr;
//! use apalis_cron::{CronStream, Schedule};
//! use chrono::{DateTime, Utc};
//!
//! #[derive(Default, Debug, Clone)]
//! struct Reminder(DateTime<Utc>);
//!
//! impl From<DateTime<Utc>> for Reminder {
//!    fn from(t: DateTime<Utc>) -> Self {
//!        Reminder(t)
//!    }
//! }
//!
//! async fn handle_tick(job: Reminder, data: Data<usize>) {
//!     // Do something with the current tick
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let schedule = Schedule::from_str("@daily").unwrap();
//!
//!     let worker = WorkerBuilder::new("morning-cereal")
//!         .retry(RetryPolicy::retries(5))
//!         .data(42usize)
//!         .backend(CronStream::new(schedule))
//!         .build_fn(handle_tick);
//!
//!     worker.run().await;
//! }
//! ```
//! ## Persisting cron jobs
//!
//! Sometimes we may want to persist cron jobs for several reasons:
//!
//! - Distribute cronjobs between multiple servers
//! - Store the results of the cronjob
//! - Prevent task skipping in the case of a restart
//!
//! ```rust,no_run
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
//!     let backend = cron_stream.pipe_to_storage(sqlite);
//!
//!     let worker = WorkerBuilder::new("morning-cereal")
//!         .backend(backend)
//!         .build_fn(handle_tick);
//!
//!     worker.run().await;
//! }
//! ```

use apalis_core::backend::Backend;
use apalis_core::error::BoxDynError;
use apalis_core::layers::Identity;
use apalis_core::mq::MessageQueue;
use apalis_core::poller::Poller;
use apalis_core::request::RequestStream;
use apalis_core::storage::Storage;
use apalis_core::task::namespace::Namespace;
use apalis_core::worker::{Context, Worker};
use apalis_core::{error::Error, request::Request};
use chrono::{DateTime, TimeZone, Utc};
pub use cron::Schedule;
use futures::StreamExt;
use pipe::CronPipe;
use std::marker::PhantomData;
use std::sync::Arc;

/// Allows piping of cronjobs to a Storage or MessageQueue
pub mod pipe;

/// Represents a stream from a cron schedule with a timezone
#[derive(Clone, Debug)]
pub struct CronStream<J, Tz> {
    schedule: Schedule,
    timezone: Tz,
    _marker: PhantomData<J>,
}

impl<J> CronStream<J, Utc> {
    /// Build a new cron stream from a schedule using the UTC timezone
    pub fn new(schedule: Schedule) -> Self {
        Self {
            schedule,
            timezone: Utc,
            _marker: PhantomData,
        }
    }
}

impl<J, Tz> CronStream<J, Tz>
where
    Tz: TimeZone + Send + Sync + 'static,
{
    /// Build a new cron stream from a schedule and timezone
    pub fn new_with_timezone(schedule: Schedule, timezone: Tz) -> Self {
        Self {
            schedule,
            timezone,
            _marker: PhantomData,
        }
    }
}
impl<Req, Tz> CronStream<Req, Tz>
where
    Req: From<DateTime<Tz>> + Send + Sync + 'static,
    Tz: TimeZone + Send + Sync + 'static,
    Tz::Offset: Send + Sync,
{
    /// Convert to consumable
    fn into_stream(self) -> RequestStream<Request<Req, ()>> {
        let timezone = self.timezone.clone();
        let stream = async_stream::stream! {
            let mut schedule = self.schedule.upcoming_owned(timezone.clone());
            loop {
                let next = schedule.next();
                match next {
                    Some(next) => {
                        let to_sleep = next - timezone.from_utc_datetime(&Utc::now().naive_utc());
                        let to_sleep = to_sleep.to_std().map_err(|e| Error::SourceError(Arc::new(e.into())))?;
                        apalis_core::sleep(to_sleep).await;
                        let timestamp = timezone.from_utc_datetime(&Utc::now().naive_utc());
                        let namespace = Namespace(format!("{}:{timestamp:?}", self.schedule));
                        let mut req = Request::new(Req::from(timestamp));
                        req.parts.namespace = Some(namespace);
                        yield Ok(Some(req));
                    },
                    None => {
                        yield Ok(None);
                    }
                }
            }
        };
        Box::pin(stream)
    }

    /// Push cron job events to a storage and get a consumable Backend
    pub fn pipe_to_storage<S, Ctx>(self, storage: S) -> CronPipe<S>
    where
        S: Storage<Job = Req, Context = Ctx> + Clone + Send + Sync + 'static,
        S::Error: std::error::Error + Send + Sync + 'static,
    {
        let stream = self
            .into_stream()
            .then({
                let storage = storage.clone();
                move |res| {
                    let mut storage = storage.clone();
                    async move {
                        match res {
                            Ok(Some(req)) => storage
                                .push(req.args)
                                .await
                                .map(|_| ())
                                .map_err(|e| Box::new(e) as BoxDynError),
                            _ => Ok(()),
                        }
                    }
                }
            })
            .boxed();

        CronPipe {
            stream,
            inner: storage,
        }
    }
    /// Push cron job events to a message queue and get a consumable Backend
    pub fn pipe_to_mq<Mq>(self, mq: Mq) -> CronPipe<Mq>
    where
        Mq: MessageQueue<Req> + Clone + Send + Sync + 'static,
        Mq::Error: std::error::Error + Send + Sync + 'static,
    {
        let stream = self
            .into_stream()
            .then({
                let mq = mq.clone();
                move |res| {
                    let mut mq = mq.clone();
                    async move {
                        match res {
                            Ok(Some(req)) => mq
                                .enqueue(req.args)
                                .await
                                .map(|_| ())
                                .map_err(|e| Box::new(e) as BoxDynError),
                            _ => Ok(()),
                        }
                    }
                }
            })
            .boxed();

        CronPipe { stream, inner: mq }
    }
}

impl<Req, Tz, Res> Backend<Request<Req, ()>, Res> for CronStream<Req, Tz>
where
    Req: From<DateTime<Tz>> + Send + Sync + 'static,
    Tz: TimeZone + Send + Sync + 'static,
    Tz::Offset: Send + Sync,
{
    type Stream = RequestStream<Request<Req, ()>>;

    type Layer = Identity;

    fn poll<Svc>(self, _worker: &Worker<Context>) -> Poller<Self::Stream, Self::Layer> {
        let stream = self.into_stream();
        Poller::new(stream, futures::future::pending())
    }
}
