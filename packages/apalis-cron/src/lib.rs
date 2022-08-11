#![crate_name = "apalis_cron"]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! # Apalis Cron
//! A simple yet extensible library for cron-like job scheduling for rust .
//!
//! ```rust
//! use apalis::prelude::*;
//! use apalis::cron::{CronWorker, Schedule};
//! use std::str::FromStr;
//! use serde::{Serialize,Deserialize};
//!
//! #[derive(Serialize, Deserialize, Default, Debug)]
//! struct Reminder;
//!
//! impl Job for Reminder {
//!     const NAME: &'static str = "reminder::DailyReminder";
//! }
//! async fn send_reminder(_job: Reminder, _ctx: JobContext) -> Result<JobResult, JobError> {
//!     Ok(JobResult::Success)
//! }
//! #[tokio::main]
//! async fn main() {
//!     let schedule = Schedule::from_str("@daily").unwrap();
//!     let worker = CronWorker::new(schedule, job_fn(send_reminder));
//!     Monitor::new()
//!         .register(worker)
//!         .run_without_signals() // run()
//!         .await
//!         .unwrap();
//! }
//! ```

use apalis_core::{
    error::JobError,
    job::{Job, JobStreamResult},
    request::JobRequest,
    response::JobResult,
    worker::prelude::*,
};
use async_stream::try_stream;
use chrono::Utc;

use futures::Future;
use std::marker::PhantomData;
use tokio::time::sleep;
use tower::Service;

pub use cron::Schedule;

/// Represents a worker that runs cron jobs
#[derive(Clone, Debug)]
pub struct CronWorker<S, J> {
    service: S,
    job_type: PhantomData<J>,
    schedule: Schedule,
}

impl<'a, S, J> CronWorker<S, J> {
    /// Creates a new [CronWorker] instance
    pub fn new(schedule: Schedule, service: S) -> Self {
        Self {
            service,
            job_type: PhantomData,
            schedule,
        }
    }
}

impl<S, F, J> Worker for CronWorker<S, J>
where
    S: 'static + Send + Service<JobRequest<J>, Response = JobResult, Error = JobError, Future = F>,
    F: Future<Output = Result<JobResult, JobError>> + Send + 'static,
    J: Job + 'static + Default,
{
    type Job = J;
    type Service = S;
    type Future = F;

    fn service(&mut self) -> &mut S {
        &mut self.service
    }

    fn consume(&mut self) -> JobStreamResult<Self::Job> {
        let shedule = self.schedule.clone();
        let stream = try_stream! {
            let mut schedule = shedule.upcoming_owned(Utc);
            loop {
                let next = schedule.next().unwrap();
                let to_sleep = next - chrono::Utc::now();
                let to_sleep = to_sleep.to_std().unwrap();
                sleep(to_sleep).await;
                yield Some(JobRequest::new(J::default()));
            }
        };
        Box::pin(stream)
    }
}

#[cfg(test)]
mod tests {

    use std::{
        ops::Deref,
        str::FromStr,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use apalis_core::{context::JobContext, job_fn::job_fn, layers::extensions::Extension};
    use tower::ServiceBuilder;

    use super::*;

    #[tokio::test]
    async fn test_cron_worker() {
        #[derive(Debug, serde::Serialize, serde::Deserialize, Default)]
        struct EmailReminder;

        #[derive(Debug)]
        struct Counter(AtomicUsize);

        impl Deref for Counter {
            type Target = AtomicUsize;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl Job for EmailReminder {
            const NAME: &'static str = "reminder::Email";
        }

        async fn send_reminder(
            _job: EmailReminder,
            ctx: JobContext,
        ) -> Result<JobResult, JobError> {
            let counter = ctx.data_opt::<Arc<Counter>>().expect("Must have counter");
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(JobResult::Success)
        }

        let counter = Arc::from(Counter(AtomicUsize::new(0)));
        let job = ServiceBuilder::new()
            .layer(Extension(counter.clone()))
            .service(job_fn(send_reminder));

        Monitor::new()
            .register({
                CronWorker::new(Schedule::from_str("1/1 * * * * *").unwrap(), job.clone())
            })
            .run_without_signals()
            .await
            .unwrap();
        sleep(Duration::from_secs(5)).await;
        assert_eq!(counter.load(Ordering::Relaxed), 5)
    }
}
