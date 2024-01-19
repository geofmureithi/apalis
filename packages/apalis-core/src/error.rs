use std::error::Error as StdError;
use thiserror::Error;

/// Convenience type alias
pub(crate) type BoxDynError = Box<dyn StdError + 'static + Send + Sync>;

/// Represents an error that is returned from an job.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// An error occurred during execution.
    #[error("Task Failed: {0}")]
    Failed(#[source] BoxDynError),

    /// A generic IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Missing some context and yet it was requested during execution.
    #[error("MissingContext: {0}")]
    InvalidContext(String),

    /// Execution was aborted
    #[error("Execution was aborted")]
    Abort,
}

/// Represents a [JobStream] error.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StreamError {
    /// An error occurred during streaming.
    #[error("Broken Pipe: {0}")]
    BrokenPipe(#[source] BoxDynError),
}
