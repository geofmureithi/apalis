use std::{error::Error as StdError, time::Duration};
use thiserror::Error;

/// Convenience type alias
pub type BoxDynError = Box<dyn StdError + 'static + Send + Sync>;
/// Execution should be aborted
/// This signifies that the task should not be retried
#[derive(Error, Debug)]
#[error("AbortError: {source}")]
pub struct AbortError {
    #[source]
    source: BoxDynError,
}

/// Execution should be retried after a specific duration
/// This increases the attempts
#[derive(Error, Debug)]
#[error("RetryError: {source}")]
pub struct RetryAfterError {
    #[source]
    source: BoxDynError,
    duration: Duration
}

/// Execution should be deferred, will be retried instantly
#[derive(Error, Debug)]
#[error("DeferredError: {source}")]
pub struct DeferredError {
    #[source]
    source: BoxDynError,
}

/// Possible errors that can occur when running a worker.
#[derive(Error, Debug)]
pub enum WorkerError {
    /// An error occurred while consuming the task stream.
    #[error("Failed to consume task stream: {0}")]
    ProcessingError(BoxDynError),
    /// An error occurred in the worker's heartbeat.
    #[error("Heartbeat error: {0}")]
    HeartbeatError(BoxDynError),
    /// An error occurred while trying to change the state of the worker.
    #[error("Failed to handle the new state: {0}")]
    StateError(WorkerStateError),
    /// A worker that terminates when .stop was called
    #[error("Worker stopped and gracefully exited")]
    GracefulExit,
}

#[derive(Error, Debug)]
pub enum WorkerStateError {
    #[error("Worker not started, did you forget to call worker.start()")]
    NotStarted,
    #[error("Worker already started")]
    AlreadyStarted,
    #[error("Worker is not running")]
    NotRunning,
    #[error("Worker is not paused")]
    NotPaused,
     #[error("Worker is shutting down")]
    ShuttingDown,
    #[error("Worker provided with invalid state {0}")]
    InvalidState(String)
}
