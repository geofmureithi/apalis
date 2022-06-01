use std::{fmt::Debug, time::Duration};

use crate::{
    error::{JobError, JobStreamError, WorkerError},
    request::JobRequest,
};
use futures::{future::BoxFuture, stream::BoxStream};
use serde::{de::DeserializeOwned, Serialize};

/// Represents a result for a [Job].
pub type JobFuture<I> = BoxFuture<'static, I>;
/// Represents a stream for [Job].
pub type JobStreamResult<T> = BoxStream<'static, Result<Option<JobRequest<T>>, JobStreamError>>;

#[derive(Debug)]
/// Represents a a wrapper for item produced from streams
pub struct JobRequestWrapper<T>(pub Result<Option<JobRequest<T>>, JobStreamError>);

/// Trait representing a job.
///
///
/// # Example
/// ```rust
/// impl Job for Email {
///     const NAME: &'static str = "apalis::Email";
/// }
/// ```
pub trait Job: Sized + Send + Unpin + Serialize + DeserializeOwned + Debug + Sync {
    /// Represents the name for job.
    const NAME: &'static str;

    /// How long it took before service was ready
    fn on_service_ready(&self, _req: &JobRequest<Self>, latency: Duration) {
        #[cfg(feature = "trace")]
        tracing::debug!(latency = ?latency, "service.ready");
    }

    /// Handle worker errors related to a job
    fn on_worker_error(&self, _req: &JobRequest<Self>, error: &WorkerError) {
        #[cfg(feature = "trace")]
        tracing::warn!(error =?error, "storage.error");
    }
}

/// Job objects that can be reconstructed from the data stored in Storage.
///
/// Implemented for all `Deserialize` objects by default by relying on SerdeJson
/// decoding.
trait JobDecodable
where
    Self: Sized,
{
    /// Decode the given Redis value into a message
    ///
    /// In the default implementation, the string value is decoded by assuming
    /// it was encoded through the Msgpack encoding.
    fn decode_job(value: &Vec<u8>) -> Result<Self, JobError>;
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
    fn encode_job(&self) -> Result<Vec<u8>, JobError>;
}

// impl<T> JobDecodable for T
// where
//     T: DeserializeOwned,
// {
//     fn decode_job(value: &Vec<u8>) -> Result<T, JobError> {
//         Ok(serde_json::from_slice(value)?)
//     }
// }

// impl<T: Serialize> JobEncodable for T {
//     fn encode_job(&self) -> Result<Vec<u8>, JobError> {
//         Ok(serde_json::to_vec(self)?)
//     }
// }

/// Represents a Stream of jobs being consumed by a Worker
pub trait JobStream {
    /// The job result
    type Job: Job;
    /// Get the stream of jobs
    fn stream(&mut self, worker_id: String, interval: Duration) -> JobStreamResult<Self::Job>;
}
