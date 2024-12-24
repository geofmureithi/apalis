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
//! Since `apalis-cron` is build on top of `apalis` which supports tower middlerware, you should be able to easily
//! add middleware such as tracing, retries, load-shed, concurrency etc.
//!
//! ## Example
//!
//! ```rust,no_run
//! # use apalis::layers::retry::RetryLayer;
//! # use apalis::layers::retry::RetryPolicy;
//! use tower::ServiceBuilder;
//! use apalis_cron::Schedule;
//! use std::str::FromStr;
//! # use apalis::prelude::*;
//! use apalis_cron::CronStream;
//! use chrono::{DateTime, Utc};
//!
//! #[derive(Clone)]
//! struct FakeService;
//! impl FakeService {
//!     fn execute(&self, item: Reminder){}
//! }
//!
//! #[derive(Default, Debug, Clone)]
//! struct Reminder(DateTime<Utc>);
//! impl From<DateTime<Utc>> for Reminder {
//!    fn from(t: DateTime<Utc>) -> Self {
//!        Reminder(t)
//!    }
//! }
//! async fn send_reminder(job: Reminder, svc: Data<FakeService>) {
//!     svc.execute(job);
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let schedule = Schedule::from_str("@daily").unwrap();
//!     let worker = WorkerBuilder::new("morning-cereal")
//!         .retry(RetryPolicy::retries(5))
//!         .data(FakeService)
//!         .backend(CronStream::new(schedule))
//!         .build_fn(send_reminder);
//!     Monitor::new()
//!         .register(worker)
//!         .run()
//!         .await
//!         .unwrap();
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
use apalis_core::{error::Error, request::Request, service_fn::FromRequest};
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
    Req: Default + Send + Sync + 'static,
    Tz: TimeZone + Send + Sync + 'static,
    Tz::Offset: Send + Sync,
{
    /// Convert to consumable
    fn into_stream(self) -> RequestStream<Request<Req, CronContext<Tz>>> {
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
                        let mut req = Request::new_with_ctx(Req::default(), CronContext::new(timestamp));
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

impl<Req, Tz, Res> Backend<Request<Req, CronContext<Tz>>, Res> for CronStream<Req, Tz>
where
    Req: Default + Send + Sync + 'static,
    Tz: TimeZone + Send + Sync + 'static,
    Tz::Offset: Send + Sync,
{
    type Stream = RequestStream<Request<Req, CronContext<Tz>>>;

    type Layer = Identity;

    fn poll<Svc>(self, _worker: &Worker<Context>) -> Poller<Self::Stream, Self::Layer> {
        let stream = self.into_stream();
        Poller::new(stream, futures::future::pending())
    }
}
