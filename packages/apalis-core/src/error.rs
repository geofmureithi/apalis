use std::error::Error as StdError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Storage encountered a connection error: {0}")]
    Connection(#[source] BoxDynError),
    #[error("Storage encountered a database error: {0}")]
    Database(#[source] BoxDynError),
}

// Convenience type alias for usage within Apalis.
// Do not make this type public.
pub type BoxDynError = Box<dyn StdError + 'static + Send + Sync>;

/// Represents an error that is returned from an job.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum JobError {
    /// A background worker has crashed.
    #[error("attempted to communicate with a crashed background worker")]
    WorkerCrashed,

    /// An error occured during execution.
    #[error("job failed with returned error: {0}")]
    Failed(#[source] BoxDynError),

    /// An error communicating with storage.
    #[error("error communicating with storage: {0}")]
    Storage(StorageError),

    /// A generic IO error
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("unknown error")]
    Unknown,
}

// #[cfg(feature = "sqlite")]
impl From<sqlx::Error> for StorageError {
    fn from(e: sqlx::Error) -> Self {
        StorageError::Database(Box::from(e))
    }
}

/// Represents a queue error.
#[derive(Debug, Error)]
pub enum QueueError {
    /// An error communicating with storage.
    #[error("error communicating with storage: {0}")]
    Storage(StorageError),
}
