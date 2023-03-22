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

/// Represent utilities for creating [Worker] instances.
///
/// [`Worker`]: crate::worker::Worker
pub mod builder;
/// Represents the [JobContext].
pub mod context;
/// Includes all possible error types.
pub mod error;
/// Includes the utilities for a job.
pub mod job;
/// Represents a service that is created from a function.
/// See more [tower::service_fn]
#[cfg(feature = "job-service")]
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

/// Represents monitoring of running workers
pub mod monitor;
/// Represents the actual executor of a [Job].
pub mod worker;
