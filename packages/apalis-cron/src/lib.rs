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
//! use chrono::Local;
//!
//! #[derive(Default, Debug, Clone)]
//! struct Reminder;
//!
//! async fn handle_tick(_: Reminder, ctx: CronContext<Local>, data: Data<usize>) {
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
use apalis_core::codec::NoopCodec;
use apalis_core::error::BoxDynError;
use apalis_core::layers::Identity;
use apalis_core::mq::MessageQueue;
use apalis_core::poller::Poller;
use apalis_core::request::RequestStream;
use apalis_core::storage::Storage;
use apalis_core::task::namespace::Namespace;
use apalis_core::worker::{Context, Worker};
use apalis_core::{error::Error, request::Request, service_fn::FromRequest};
use chrono::{DateTime, OutOfRangeError, TimeZone, Utc};
pub use cron::Schedule;
use futures::StreamExt;
use pipe::CronPipe;
use std::fmt::{self, Debug};
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

fn build_stream<Tz: TimeZone, Req>(
    timezone: &Tz,
    schedule: &Schedule,
) -> RequestStream<Request<Req, CronContext<Tz>>>
where
    Req: Default + Send + Sync + 'static,
    Tz: TimeZone + Send + Sync + 'static,
    Tz::Offset: Send + Sync,
{
    let timezone = timezone.clone();
    let schedule = schedule.clone();
    let mut queue_schedule = schedule.upcoming_owned(timezone.clone());
    let stream = async_stream::stream! {
        loop {
            let next = queue_schedule.next();
            match next {
                Some(tick) => {
                    let to_sleep = tick.clone() - timezone.from_utc_datetime(&Utc::now().naive_utc());
                    let to_sleep_res = to_sleep.to_std();
                    match to_sleep_res {
                        Ok(to_sleep) => {
                            apalis_core::sleep(to_sleep).await;
                            let timestamp = timezone.from_utc_datetime(&Utc::now().naive_utc());
                            let namespace = Namespace(format!("{}:{timestamp:?}", schedule));
                            let mut req = Request::new_with_ctx(Default::default(), CronContext { timestamp });
                            req.parts.namespace = Some(namespace);
                            yield Ok(Some(req));
                        },
                        Err(e) => {
                            yield Err(Error::SourceError(Arc::new(Box::new(CronStreamError::OutOfRangeError { inner: e, tick }))))
                        },
                    }


                },
                None => {
                    yield Ok(None);
                }
            }
        }
    };
    stream.boxed()
}
impl<Req, Tz> CronStream<Req, Tz>
where
    Req: Default + Send + Sync + 'static,
    Tz: TimeZone + Send + Sync + 'static,
    Tz::Offset: Send + Sync,
{
    /// Convert to consumable
    fn into_stream(self) -> RequestStream<Request<Req, CronContext<Tz>>> {
        build_stream(&self.timezone, &self.schedule)
    }

    fn into_stream_worker(
        self,
        worker: &Worker<Context>,
    ) -> RequestStream<Request<Req, CronContext<Tz>>> {
        let worker = worker.clone();
        let mut poller = build_stream(&self.timezone, &self.schedule);
        let stream = async_stream::stream! {
            loop {
                if worker.is_shutting_down() {
                    break;
                }
                match poller.next().await {
                    Some(res) => yield res,
                    None => break,
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

/// Context for all cron jobs
#[derive(Debug, Clone)]
pub struct CronContext<Tz: TimeZone> {
    timestamp: DateTime<Tz>,
}

impl<Tz: TimeZone> Default for CronContext<Tz>
where
    DateTime<Tz>: Default,
{
    fn default() -> Self {
        Self {
            timestamp: Default::default(),
        }
    }
}

impl<Tz: TimeZone> CronContext<Tz> {
    /// Create a new context provided a timestamp
    pub fn new(timestamp: DateTime<Tz>) -> Self {
        Self { timestamp }
    }

    /// Get the inner timestamp
    pub fn get_timestamp(&self) -> &DateTime<Tz> {
        &self.timestamp
    }
}

impl<Req, Tz: TimeZone> FromRequest<Request<Req, CronContext<Tz>>> for CronContext<Tz> {
    fn from_request(req: &Request<Req, CronContext<Tz>>) -> Result<Self, Error> {
        Ok(req.parts.context.clone())
    }
}

impl<Req, Tz> Backend<Request<Req, CronContext<Tz>>> for CronStream<Req, Tz>
where
    Req: Default + Send + Sync + 'static,
    Tz: TimeZone + Send + Sync + 'static,
    Tz::Offset: Send + Sync,
{
    type Stream = RequestStream<Request<Req, CronContext<Tz>>>;

    type Layer = Identity;

    type Codec = NoopCodec<Request<Req, CronContext<Tz>>>;

    fn poll(self, worker: &Worker<Context>) -> Poller<Self::Stream, Self::Layer> {
        let stream = self.into_stream_worker(worker);
        Poller::new(stream, futures::future::pending())
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

impl<Tz: TimeZone> fmt::Display for CronStreamError<Tz> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CronStreamError::OutOfRangeError { inner, tick } => {
                write!(
                    f,
                    "Cron tick {} is out of range: {}",
                    tick.timestamp(),
                    inner
                )
            }
        }
    }
}

impl<Tz: TimeZone> std::error::Error for CronStreamError<Tz> {
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
