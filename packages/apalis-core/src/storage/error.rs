use thiserror::Error;

use crate::error::{BoxDynError};

/// Represents a storage emitted by a worker
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StorageError {
    /// Storage encountered a connection error
    #[error("Storage encountered a connection error: {0}")]
    Connection(#[source] BoxDynError),
    /// Storage encountered a database error
    #[error("Storage encountered a database error: {0}")]
    Database(#[source] BoxDynError),
    /// The resource was not found in storage
    #[error("The resource was not found in storage")]
    NotFound,
    /// Parsing Error
    #[error("Parsing Error")]
    Parse(#[source] BoxDynError),
}
