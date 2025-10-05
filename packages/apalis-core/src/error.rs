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
impl AbortError {
    /// Create a new abort error
    pub fn new<E: Into<BoxDynError>>(err: E) -> Self {
        AbortError { source: err.into() }
    }
}

/// Execution should be retried after a specific duration
/// This increases the attempts
#[derive(Error, Debug)]
#[error("RetryError: {source}")]
pub struct RetryAfterError {
    #[source]
    source: BoxDynError,
    duration: Duration,
}

impl RetryAfterError {
    /// Create a new retry after error
    pub fn new<E: Into<BoxDynError>>(err: E, duration: Duration) -> Self {
        RetryAfterError {
            source: err.into(),
            duration,
        }
    }

    /// Get the duration after which the task should be retried
    pub fn get_duration(&self) -> Duration {
        self.duration
    }
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
    StreamError(BoxDynError),
    /// An error occurred in the worker's heartbeat.
    #[error("Heartbeat error: {0}")]
    HeartbeatError(BoxDynError),
    /// An error occurred while trying to change the state of the worker.
    #[error("Failed to handle the new state: {0}")]
    StateError(WorkerStateError),
    /// A worker that terminates when .stop was called
    #[error("Worker stopped and gracefully exited")]
    GracefulExit,
    /// A worker panicked and the panic was caught.
    #[error("Worker panicked: {0}")]
    PanicError(String),
    /// An error occurred while handling io
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

/// Errors related to worker state transitions
#[derive(Error, Debug)]
pub enum WorkerStateError {
    /// Worker not started
    #[error("Worker not started, did you forget to call worker.start()")]
    NotStarted,
    /// Worker already started
    #[error("Worker already started")]
    AlreadyStarted,
    /// Worker is not running
    #[error("Worker is not running")]
    NotRunning,
    /// Worker is not paused
    #[error("Worker is not paused")]
    NotPaused,
    /// Worker is shutting down
    #[error("Worker is shutting down")]
    ShuttingDown,
    /// Invalid state provided
    #[error("Worker provided with invalid state {0}")]
    InvalidState(String),
}
