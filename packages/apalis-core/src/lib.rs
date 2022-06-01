#![crate_name = "apalis_core"]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! # Apalis Core
//! Utilities for building job and message processing tools.
//!
//! ```rust
//! use futures::Future;
//! use tower::Service;
//! use apalis_core::{
//!    context::JobContext,
//!    error::JobError,
//!    job::{Job, JobStreamResult},
//!    job_fn::job_fn,
//!    request::JobRequest,
//!    response::JobResult,
//!    worker::prelude::*,
//! };
//! async fn main() {
//!     struct SimpleWorker<S>(S);
//!
//!     #[derive(Debug, serde::Serialize, serde::Deserialize)]
//!     struct Email;
//!
//!     impl Job for Email {
//!         const NAME: &'static str = "worker::Email";
//!     }
//!
//!     async fn send_email(job: Email, _ctx: JobContext) -> Result<JobResult, JobError> {
//!         Ok(JobResult::Success)
//!     }
//!
//!     impl<S, F> Worker for SimpleWorker<S>
//!     where
//!         S: 'static
//!             + Send
//!             + Service<JobRequest<Email>, Response = JobResult, Error = JobError, Future = F>,
//!         F: Future<Output = Result<JobResult, JobError>> + Send + 'static,
//!     {
//!         type Job = Email;
//!         type Service = S;
//!         type Future = F;
//!
//!         fn service(&mut self) -> &mut S {
//!             &mut self.0
//!         }
//!
//!         fn consume(&mut self) -> JobStreamResult<Self::Job> {
//!             use futures::stream;
//!             let stream = stream::iter(vec![
//!                 Ok(Some(JobRequest::new(Email))),
//!                 Ok(Some(JobRequest::new(Email))),
//!                 Ok(Some(JobRequest::new(Email))),
//!             ]);
//!             Box::pin(stream)
//!         }
//!     }
//!     Monitor::new()
//!         .register_with_count(1, move |_| SimpleWorker(job_fn(send_email)))
//!         .run()
//!         .await;
//! }

/// Represent utilities for creating [Worker] instances.
pub mod builder;
/// Represents the [JobContext].
pub mod context;
/// Includes all possible error types.
pub mod error;
/// Includes the utilities for a job.
pub mod job;
/// Represents a service that is created from a function.
/// See more [tower::service_fn]
pub mod job_fn;
/// Represents middleware offered through [tower::Layer]
pub mod layers;
/// Represents the job bytes.
pub mod request;
/// Represents different possible responses.
pub mod response;

#[cfg(feature = "storage")]
#[cfg_attr(docsrs, doc(cfg(feature = "storage")))]
/// Represents ability to persist and consume jobs from storages.
pub mod storage;

#[cfg(feature = "worker")]
#[cfg_attr(docsrs, doc(cfg(feature = "worker")))]
/// Represents the actual executor of a [Job].
pub mod worker;
