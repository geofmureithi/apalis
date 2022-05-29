#![crate_name = "apalis_core"]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]

/// Represent utilities for creating [Worker] instances
pub mod builder;
/// Represents the [JobContext]
pub mod context;
/// Includes all possible error types
pub mod error;
/// Includes the utilities for a job
pub mod job;

pub mod job_fn;
/// Represents middleware offered through [tower::Layer]
pub mod layers;
/// Represents the job bytes and context
pub mod request;
/// Represents different possible responses
pub mod response;

#[cfg(feature = "storage")]
#[cfg_attr(docsrs, doc(cfg(feature = "storage")))]
/// Represents ability to manipulate storages.
pub mod storage;

/// Represents the actual executor of a job.
pub mod worker;
