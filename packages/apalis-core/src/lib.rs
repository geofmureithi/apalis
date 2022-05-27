#![crate_name = "apalis_core"]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "actix-runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "actix-runtime")))]
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
#[cfg(feature = "actix-runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "actix-runtime")))]
/// Monitors multiple [Worker] instances
pub mod monitor;
/// Represents the job bytes and context
pub mod request;
/// Represents different possible responses
pub mod response;

#[cfg(feature = "storage")]
#[cfg_attr(docsrs, doc(cfg(feature = "storage")))]
/// Represents ability to manipulate storages.
pub mod storage;

#[cfg(feature = "actix-runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "actix-runtime")))]
/// Represents the different heartbeats between [Worker] and [Storage]
pub mod streams;

#[cfg(feature = "actix-runtime")]
#[cfg_attr(docsrs, doc(cfg(feature = "actix-runtime")))]
/// Represents the actual executor of a job.
pub mod worker;
