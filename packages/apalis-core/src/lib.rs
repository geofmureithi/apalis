#![crate_name = "apalis_core"]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub,
    bad_style,
    dead_code,
    improper_ctypes,
    non_shorthand_field_patterns,
    no_mangle_generic_items,
    overflowing_literals,
    path_statements,
    patterns_in_fns_without_body,
    unconditional_recursion,
    unused,
    unused_allocation,
    unused_comparisons,
    unused_parens,
    while_true
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! # apalis-core
//! Utilities for building job and message processing tools.
use futures::Stream;
use poller::Poller;
use worker::WorkerId;

/// Represent utilities for creating worker instances.
pub mod builder;
/// Includes all possible error types.
pub mod error;
/// Represents an executor.
pub mod executor;
/// Represents middleware offered through [`tower`]
pub mod layers;
/// Represents monitoring of running workers
pub mod monitor;
/// Represents the request to be processed.
pub mod request;
/// Represents different possible responses.
pub mod response;
/// Represents a service that is created from a function.
pub mod service_fn;
/// Represents ability to persist and consume jobs from storages.
pub mod storage;
/// Represents the utils for building workers.
pub mod worker;

/// Represents the utils needed to extend a task's context.
pub mod data;
/// Message queuing utilities
pub mod mq;
/// Allows async listening in a mpsc style.
pub mod notify;
/// Controlled polling and streaming
pub mod poller;

/// In-memory utilities for testing and mocking
pub mod memory;

/// Task management utilities
pub mod task;

/// Codec for handling data
pub mod codec;

/// A backend represents a task source
/// Both [`Storage`] and [`MessageQueue`] need to implement it for workers to be able to consume tasks
///
/// [`Storage`]: crate::storage::Storage
/// [`MessageQueue`]: crate::mq::MessageQueue
pub trait Backend<Req> {
    /// The stream to be produced by the backend
    type Stream: Stream<Item = Result<Option<Req>, crate::error::Error>>;

    /// Returns the final decoration of layers
    type Layer;

    /// Allows the backend to decorate the service with [Layer]
    ///
    /// [Layer]: tower::Layer
    #[allow(unused)]
    fn common_layer(&self, worker: WorkerId) -> Self::Layer;

    /// Returns a poller that is ready for streaming
    fn poll(self, worker: WorkerId) -> Poller<Self::Stream>;
}

/// This allows encoding and decoding of requests in different backends
pub trait Codec<T, Compact> {
    /// Error encountered by the codec
    type Error;

    /// Convert to the compact version
    fn encode(&self, input: &T) -> Result<Compact, Self::Error>;

    /// Decode back to our request type
    fn decode(&self, compact: &Compact) -> Result<T, Self::Error>;
}

/// Sleep utilities
#[cfg(feature = "sleep")]
pub async fn sleep(duration: std::time::Duration) {
    let mut interval = async_timer::Interval::platform_new(duration);
    interval.wait().await;
}

#[cfg(test)]
#[doc(hidden)]
#[derive(Debug, Default, Clone)]
pub(crate) struct TestExecutor;
#[cfg(test)]
impl crate::executor::Executor for TestExecutor {
    fn spawn(&self, future: impl futures::prelude::Future<Output = ()> + Send + 'static) {
        tokio::spawn(future);
    }
}
