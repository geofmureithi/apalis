use std::{
    error::Error as StdError,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use thiserror::Error;
use tower::Service;

use crate::worker::WorkerError;

/// Convenience type alias
pub type BoxDynError = Box<dyn StdError + 'static + Send + Sync>;
/// Execution should be aborted
/// This signifies that the task should not be retried
#[derive(Error, Debug)]
#[error("AbortError: {reason}, source: {source}")]
pub struct AbortError {
    reason: String,
    #[source]
    source: BoxDynError,
}

/// Execution should be retried after a specific duration
/// This increases the attempts
#[derive(Error, Debug)]
#[error("RetryError: {reason}, source: {source}")]
pub struct RetryError {
    reason: String,
    #[source]
    source: BoxDynError,
}

/// Execution should be retried after a specific duration
#[derive(Error, Debug)]
#[error("DeferredError: {reason}, source: {source}")]
pub struct DeferredError {
    reason: String,
    #[source]
    source: BoxDynError,
}
