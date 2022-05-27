use std::error::Error as StdError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Storage encountered a connection error: {0}")]
    Connection(#[source] BoxDynError),
    #[error("Storage encountered a database error: {0}")]
    Database(#[source] BoxDynError),
    #[error("The resource was not found in storage")]
    NotFound,
    #[error("Serialization/Deserialization Error")]
    SerDe(#[source] BoxDynError),
}

/// Convenience type alias for usage within Apalis.
///
/// Do not make this type public.
pub type BoxDynError = Box<dyn StdError + 'static + Send + Sync>;

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

    /// An error communicating with storage.
    #[error("Error communicating with storage: {0}")]
    Storage(StorageError),

    /// A generic IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Unknown error")]
    Unknown,
}

impl From<serde_json::Error> for StorageError {
    fn from(e: serde_json::Error) -> Self {
        StorageError::SerDe(Box::from(e))
    }
}

/// Represents a queue error.
#[derive(Debug, Error)]
pub enum WorkerError {
    /// An error communicating with storage.
    #[error("error communicating with storage: {0}")]
    Storage(StorageError),
}
