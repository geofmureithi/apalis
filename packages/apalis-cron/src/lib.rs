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
//! add middleware such as tracing, retries, load shed, concurrency etc.
//!
//! ## Example
//!
//! ```rust,no_run
//! # use apalis_core::context::JobContext;
//! # use apalis_core::layers::retry::RetryLayer;
//! # use apalis_core::layers::retry::DefaultRetryPolicy;
//! # use apalis_core::layers::extensions::Extension;
//! # use apalis_core::job_fn::job_fn;
//! # use apalis_core::job::Job;
//! use tower::ServiceBuilder;
//! use apalis_cron::Schedule;
//! use std::str::FromStr;
//! # use apalis_core::monitor::Monitor;
//! # use apalis_core::builder::WorkerBuilder;
//! # use apalis_core::builder::WorkerFactory;
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
//! impl Job for Reminder {
//!     const NAME: &'static str = "reminder::DailyReminder";
//! }
//! async fn send_reminder(job: Reminder, ctx: JobContext) {
//!     let svc = ctx.data_opt::<FakeService>().unwrap();
//!     svc.execute(job);
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let schedule = Schedule::from_str("@daily").unwrap();
//!     let service = ServiceBuilder::new()
//!         .layer(RetryLayer::new(DefaultRetryPolicy))
//!         .layer(Extension(FakeService))
//!         .service(job_fn(send_reminder));
//!     let worker = WorkerBuilder::new("morning-cereal")
//!         .stream(CronStream::new(schedule).to_stream())
//!         .build(service);
//!     Monitor::new()
//!         .register(worker)
//!         .run()
//!         .await
//!         .unwrap();
//! }
//! ```

use apalis_core::job::Job;
use apalis_core::utils::Timer;
use apalis_core::{error::JobError, request::JobRequest};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
pub use cron::Schedule;
use futures::stream::BoxStream;
use futures::StreamExt;
use std::marker::PhantomData;

/// Represents a stream from a cron schedule
#[derive(Clone, Debug)]
pub struct CronStream<J, T>(Schedule, PhantomData<J>, T);

impl<J: From<DateTime<Utc>> + Job + Send + 'static> CronStream<J, ()> {
    /// Build a new cron stream from a schedule
    pub fn new(schedule: Schedule) -> Self {
        Self(schedule, PhantomData, ())
    }

    /// Set a custom timer
    pub fn timer<NT: Timer>(self, timer: NT) -> CronStream<J, NT> {
        CronStream(self.0, PhantomData, timer)
    }
}

impl<J: From<DateTime<Utc>> + Job + Send + 'static, T: Timer + Sync + Send + 'static>
    CronStream<J, T>
{
    /// Convert to consumable
    pub fn to_stream(self) -> BoxStream<'static, Result<Option<JobRequest<J>>, JobError>> {
        let stream = async_stream::stream! {
            let mut schedule = self.0.upcoming_owned(Utc);
            loop {
                let next = schedule.next();
                match next {
                    Some(next) => {
                        let to_sleep = next - chrono::Utc::now();
                        let to_sleep = to_sleep.to_std().map_err(|e| JobError::Failed(e.into()))?;
                        self.2.sleep(to_sleep).await;
                        yield Ok(Some(JobRequest::new(J::from(chrono::Utc::now()))));
                    },
                    None => {
                        yield Ok(None);
                    }
                }

            }
        };
        stream.boxed()
    }
}

impl<J, T> CronStream<J, T> {
    /// Convert to consumable
    pub fn to_stream_with_timezone<Tz: TimeZone + Send + Sync + 'static>(
        self,
        timezone: Tz,
    ) -> BoxStream<'static, Result<Option<JobRequest<J>>, JobError>>
    where
        J: From<DateTime<Tz>> + Job + Send + 'static,
        T: Timer + Sync + Send + 'static,
        <Tz as TimeZone>::Offset: std::marker::Send,
    {
        let stream = async_stream::stream! {
            let mut schedule = self.0.upcoming_owned(timezone.clone());
            loop {
                let next = schedule.next();
                match next {
                    Some(next) => {
                        let to_sleep = next - timezone.from_utc_datetime(&Utc::now().naive_utc());
                        let to_sleep = to_sleep.to_std().map_err(|e| JobError::Failed(e.into()))?;
                        self.2.sleep(to_sleep).await;
                        yield Ok(Some(JobRequest::new(J::from(timezone.from_utc_datetime(&Utc::now().naive_utc())))));
                    },
                    None => {
                        yield Ok(None);
                    }
                }

            }
        };
        stream.boxed()
    }
}
