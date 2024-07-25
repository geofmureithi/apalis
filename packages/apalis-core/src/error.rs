use std::{error::Error as StdError, sync::Arc};
use thiserror::Error;

use crate::worker::WorkerError;

/// Convenience type alias
pub type BoxDynError = Box<dyn StdError + 'static + Send + Sync>;

/// Represents a general error returned by a task or by internals of the platform
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum Error {
    /// An error occurred during execution.
    #[error("FailedError: {0}")]
    Failed(#[source] Arc<BoxDynError>),

    /// A generic IO error
    #[error("IoError: {0}")]
    Io(#[from] Arc<std::io::Error>),

    /// Missing some context and yet it was requested during execution.
    #[error("MissingContextError: {0}")]
    MissingContext(String),

    /// Execution was aborted
    #[error("AbortError: {0}")]
    Abort(String),

    /// Encountered an error during worker execution
    #[error("WorkerError: {0}")]
    WorkerError(WorkerError),

    #[doc(hidden)]
    /// Encountered an error during service execution
    /// This should not be used inside a task function
    #[error("Encountered an error during service execution")]
    ServiceError(#[source] Arc<BoxDynError>),

    #[doc(hidden)]
    /// Encountered an error during service execution
    /// This should not be used inside a task function
    #[error("Encountered an error during streaming")]
    SourceError(#[source] Arc<BoxDynError>),
}
