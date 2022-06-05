use std::{collections::HashMap, fmt::Debug, time::Duration};

use crate::{
    error::{JobError, JobStreamError, WorkerError},
    request::{JobRequest, JobState},
};
use chrono::{DateTime, Utc};
use futures::{future::BoxFuture, stream::BoxStream};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// Represents a result for a [Job].
pub type JobFuture<I> = BoxFuture<'static, I>;
/// Represents a stream for [Job].
pub type JobStreamResult<T> = BoxStream<'static, Result<Option<JobRequest<T>>, JobStreamError>>;

#[derive(Debug)]
/// Represents a wrapper for a job produced from streams
pub struct JobRequestWrapper<T>(pub Result<Option<JobRequest<T>>, JobStreamError>);

/// Trait representing a job.
///
///
/// # Example
/// ```rust,ignore
/// impl Job for Email {
///     const NAME: &'static str = "apalis::Email";
/// }
/// ```
pub trait Job: Sized + Send + Unpin + Serialize + DeserializeOwned + Debug + Sync {
    /// Represents the name for job.
    const NAME: &'static str;

    /// How long it took before service was ready
    fn on_service_ready(&self, _req: &JobRequest<Self>, _latency: Duration) {
        #[cfg(feature = "trace")]
        tracing::debug!(latency = ?_latency, "service.ready");
    }

    /// Handle worker errors related to a job
    fn on_worker_error(&self, _req: &JobRequest<Self>, _error: &WorkerError) {
        #[cfg(feature = "trace")]
        tracing::warn!(error =?_error, "storage.error");
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

/// A serializable vesrion of a worker.
#[derive(Debug, Serialize, Deserialize)]
pub struct JobStreamWorker {
    /// The Worker's Id
    worker_id: String,
    /// Target for the worker, useful for display and filtering
    /// uses [std::any::type_name]
    job_type: String,
    // TODO: Add a Source type to Worker trait.
    /// The type of job stream
    source: String,
    /// The layers that were loaded for worker. uses [std::any::type_name]
    layers: String,
    /// The last time the worker was seen. [Storage] has keep alive.
    last_seen: DateTime<Utc>,
}

impl JobStreamWorker {
    /// Build a worker representation for serialization
    pub fn new<S, T>(worker_id: String, last_seen: DateTime<Utc>) -> Self
    where
        S: JobStream<Job = T>,
    {
        JobStreamWorker {
            worker_id,
            job_type: std::any::type_name::<T>().to_string(),
            source: std::any::type_name::<S>().to_string(),
            layers: String::new(),
            last_seen,
        }
    }
    /// Set layers
    pub fn set_layers(&mut self, layers: String) {
        self.layers = layers;
    }
}

/// Counts of different job states
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Counts {
    /// Counts are flattened from a Hashmap of [JobState]
    #[serde(flatten)]
    pub inner: HashMap<JobState, i64>,
}

/// JobStream extension usually useful for management via cli, web etc
#[async_trait::async_trait]

pub trait JobStreamExt<Job>: JobStream<Job = Job>
where
    Self: Sized,
{
    /// List all Workers that are working on a Job Stream
    async fn list_workers(&mut self) -> Result<Vec<JobStreamWorker>, JobError>;

    /// Returns the counts of jobs in different states
    async fn counts(&mut self) -> Result<Counts, JobError> {
        Ok(Counts {
            ..Default::default()
        })
    }

    /// Fetch jobs persisted from storage
    async fn list_jobs(
        &mut self,
        status: &JobState,
        page: i32,
    ) -> Result<Vec<JobRequest<Job>>, JobError>;
}
