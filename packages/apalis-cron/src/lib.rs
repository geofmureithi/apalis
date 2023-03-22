#![crate_name = "apalis_cron"]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! # Apalis Cron
//! A simple yet extensible library for cron-like job scheduling for rust.
//! Since `apalis-cron` is build on top of `apalis` which supports tower middlerware, you should be able to easily
//! add middleware such as tracing, retries, load shed, concurrency etc.
//!
//! ## Example
//!
//! ```rust,no_run
//! use apalis_core::context::JobContext;
//! use apalis_core::layers::retry::RetryLayer;
//! use apalis_core::layers::retry::DefaultRetryPolicy;
//! use apalis_core::layers::extensions::Extension;
//! use apalis_core::job_fn::job_fn;
//! use apalis_core::job::Job;
//! use tower::ServiceBuilder;
//! use apalis_cron::Schedule;
//! use std::str::FromStr;
//! use serde::{Serialize,Deserialize};
//! use apalis_core::monitor::Monitor;
//! use apalis_core::builder::WorkerBuilder;
//! use apalis_core::builder::WorkerFactory;
//! use apalis_cron::CronStream;
//! #[derive(Default, Clone)]
//! struct Reminder;
//!
//! impl Job for Reminder {
//!     const NAME: &'static str = "reminder::DailyReminder";
//! }
//! async fn send_reminder(job: Reminder, ctx: JobContext) {
//!     // Do reminder stuff
//!     let db = ctx.data_opt::<FakeService>().unwrap();
//!     db.push(job);
//!     
//! }
//!
//! #[derive(Clone)]
//! struct FakeService;
//!
//! impl FakeService {
//!     fn push(&self, item: Reminder){}
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let schedule = Schedule::from_str("@daily").unwrap();
//!     let service = ServiceBuilder::new()
//!         .layer(RetryLayer::new(DefaultRetryPolicy))
//!         .layer(Extension(FakeService))
//!         .service(job_fn(send_reminder));
//!
//!     let worker = WorkerBuilder::new("morning-cereal").stream(CronStream::new(schedule).to_stream()).build(service);
//!     Monitor::new()
//!         .register(worker)
//!         .run()
//!         .await
//!         .unwrap();
//! }
//! ```

use apalis_core::{error::JobError, request::JobRequest};
use chrono::Utc;
pub use cron::Schedule;
use futures::stream::BoxStream;
use futures::StreamExt;
use std::marker::PhantomData;
use tokio::time::sleep;

/// Represents a stream from a cron schedule
#[derive(Clone, Debug)]
pub struct CronStream<J>(Schedule, PhantomData<J>);

impl<T: Default + Send + 'static> CronStream<T> {
    /// Build a new cron stream from a schedule
    pub fn new(schedule: Schedule) -> Self {
        Self(schedule, PhantomData)
    }

    /// Convert to consumable
    pub fn to_stream(self) -> BoxStream<'static, Result<Option<JobRequest<T>>, JobError>> {
        let stream = async_stream::stream! {
            let mut schedule = self.0.upcoming_owned(Utc);
            loop {
                let next = schedule.next();
                match next {
                    Some(next) => {
                        let to_sleep = next - chrono::Utc::now();
                        let to_sleep = to_sleep.to_std().unwrap();
                        sleep(to_sleep).await;
                        yield Ok(Some(JobRequest::new(T::default())));
                    },
                    None => {

                    }
                }

            }
        };
        stream.boxed()
    }
}
