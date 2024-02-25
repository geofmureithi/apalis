use std::error::Error as StdError;
use thiserror::Error;

use crate::worker::WorkerError;

/// Convenience type alias
pub type BoxDynError = Box<dyn StdError + 'static + Send + Sync>;

/// Represents a general error returned by a task or by internals of the platform
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

    /// Encountered an error during worker execution
    #[error("Encountered an error during worker execution")]
    WorkerError(WorkerError),

    #[doc(hidden)]
    /// Encountered an error during service execution
    /// This should not be used inside a task function
    #[error("Encountered an error during service execution")]
    ServiceError(#[source] BoxDynError),

    #[doc(hidden)]
    /// Encountered an error during service execution
    /// This should not be used inside a task function
    #[error("Encountered an error during streaming")]
    SourceError(#[source] BoxDynError),
}
