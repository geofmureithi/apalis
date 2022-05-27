use std::time::Duration;

use crate::{error::StorageError, request::JobRequest};
use futures::future::BoxFuture;
use serde::{de::DeserializeOwned, Serialize};

/// Represents a result for a [Job] executed via [JobService].
pub type JobFuture<I> = BoxFuture<'static, I>;

/// Trait representing a job.
///
///
/// # Example
/// ```rust
/// impl Job for Email {
///     const NAME: &'static str = "apalis::Email";
/// }
/// ```
pub trait Job: Sized + Send + Unpin {
    /// Represents the name for job.
    const NAME: &'static str;

    /// How long it took before service was ready
    fn on_service_ready(&self, _req: &JobRequest<Self>, latency: Duration) {
        #[cfg(feature = "trace")]
        tracing::debug!(latency = ?latency, "service.ready");
    }
    /// Get notified when a job is cleaned
    fn on_clean_up(&self, _req: &JobRequest<Self>, _result: &Result<(), StorageError>) {
        #[cfg(feature = "trace")]
        tracing::debug!("process.cleanup");
    }

    /// Handle storage errors
    fn on_storage_error(&self, _req: &JobRequest<Self>, error: &StorageError) {
        #[cfg(feature = "trace")]
        tracing::warn!(error =?error, "storage.error");
    }
}

/// Job objects that can be reconstructed from the data stored in Storage.
///
/// Implemented for all `Deserialize` objects by default by relying on Msgpack
/// decoding.
trait JobDecodable
where
    Self: Sized,
{
    /// Decode the given Redis value into a message
    ///
    /// In the default implementation, the string value is decoded by assuming
    /// it was encoded through the Msgpack encoding.
    fn decode_job(value: &Vec<u8>) -> Result<Self, StorageError>;
}

/// Job objects that can be encoded to a string to be stored in Storage.
///
/// Implemented for all `Serialize` objects by default by encoding with Serde.
trait JobEncodable
where
    Self: Sized,
{
    /// Encode the value into a bytes array to be inserted into Storage.
    ///
    /// In the default implementation, the object is encoded with Serde.
    fn encode_job(&self) -> Result<Vec<u8>, StorageError>;
}

impl<T> JobDecodable for T
where
    T: DeserializeOwned,
{
    fn decode_job(value: &Vec<u8>) -> Result<T, StorageError> {
        Ok(serde_json::from_slice(value)?)
    }
}

impl<T: Serialize> JobEncodable for T {
    fn encode_job(&self) -> Result<Vec<u8>, StorageError> {
        Ok(serde_json::to_vec(self)?)
    }
}
