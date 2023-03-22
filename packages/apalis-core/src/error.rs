use std::error::Error as StdError;
use thiserror::Error;

#[cfg(feature = "storage")]
#[cfg_attr(docsrs, doc(cfg(feature = "storage")))]
use crate::storage::StorageError;

/// Convenience type alias for usage within Apalis.
///
pub(crate) type BoxDynError = Box<dyn StdError + 'static + Send + Sync>;

/// Represents an error that is returned from an job.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum JobError {
    /// A background worker has crashed.
    #[error("Attempted to communicate with a crashed background worker")]
    WorkerCrashed,

    /// An error occured during execution.
    #[error("Job Failed: {0}")]
    Failed(#[source] BoxDynError),

    #[cfg(feature = "storage")]
    #[cfg_attr(docsrs, doc(cfg(feature = "storage")))]
    /// An error communicating with storage.
    #[error("Error communicating with storage: {0}")]
    Storage(StorageError),

    /// A generic IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// An unclear error
    #[error("Unknown error")]
    Unknown,
}

/// Represents a [JobStream] error.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum JobStreamError {
    /// An error occured during streaming.
    #[error("Broken Pipe: {0}")]
    BrokenPipe(#[source] BoxDynError),
}
