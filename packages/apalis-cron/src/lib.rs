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
//! # use apalis_utils::layers::retry::RetryLayer;
//! # use apalis_utils::layers::retry::DefaultRetryPolicy;
//! # use apalis_core::extensions::Data;
//! # use apalis_core::service_fn::service_fn;
//! use tower::ServiceBuilder;
//! use apalis_cron::Schedule;
//! use std::str::FromStr;
//! # use apalis_core::monitor::Monitor;
//! # use apalis_core::builder::WorkerBuilder;
//! # use apalis_core::builder::WorkerFactoryFn;
//! # use apalis_utils::TokioExecutor;
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
//!         .layer(RetryLayer::new(DefaultRetryPolicy))
//!         .data(FakeService)
//!         .stream(CronStream::new(schedule).into_stream())
//!         .build_fn(send_reminder);
//!     Monitor::<TokioExecutor>::new()
//!         .register(worker)
//!         .run()
//!         .await
//!         .unwrap();
//! }
//! ```

use apalis_core::data::Extensions;
use apalis_core::request::RequestStream;
use apalis_core::task::task_id::TaskId;
use apalis_core::{error::Error, request::Request};
use chrono::{DateTime, TimeZone, Utc};
pub use cron::Schedule;
use std::marker::PhantomData;

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
impl<J, Tz> CronStream<J, Tz>
where
    J: From<DateTime<Tz>> + Send + Sync + 'static,
    Tz: TimeZone + Send + Sync + 'static,
    Tz::Offset: Send + Sync,
{
    /// Convert to consumable
    pub fn into_stream(self) -> RequestStream<Request<J>> {
        let timezone = self.timezone.clone();
        let stream = async_stream::stream! {
            let mut schedule = self.schedule.upcoming_owned(timezone.clone());
            loop {
                let next = schedule.next();
                match next {
                    Some(next) => {
                        let to_sleep = next - timezone.from_utc_datetime(&Utc::now().naive_utc());
                        let to_sleep = to_sleep.to_std().map_err(|e| Error::Failed(e.into()))?;
                        apalis_core::sleep(to_sleep).await;
                        let mut data = Extensions::new();
                        data.insert(TaskId::new());
                        yield Ok(Some(Request::new_with_data(J::from(timezone.from_utc_datetime(&Utc::now().naive_utc())), data)));
                    },
                    None => {
                        yield Ok(None);
                    }
                }
            }
        };
        Box::pin(stream)
    }
}
