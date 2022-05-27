use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::{future::BoxFuture, stream::BoxStream};

use crate::{
    error::StorageError,
    job::Job,
    request::{JobRequest, JobState},
    worker::WorkerPulse,
};

pub type StorageResult<I> = BoxFuture<'static, Result<I, StorageError>>;
pub type JobStream<T> = BoxStream<'static, Result<Option<JobRequest<T>>, StorageError>>;

/// Represents a [Storage] that can be passed to a [crate::builder::WorkerBuilder]
pub trait Storage: Clone {
    type Output: Job;

    /// Pushes a job to a storage
    ///
    /// TODO: return id
    fn push(&mut self, job: Self::Output) -> StorageResult<()>;

    fn schedule(&mut self, job: Self::Output, on: DateTime<Utc>) -> StorageResult<()>;

    fn len(&self) -> StorageResult<i64>;

    fn fetch_by_id(&self, job_id: String) -> StorageResult<Option<JobRequest<Self::Output>>>;

    /// Get the stream of jobs
    fn consume(&mut self, worker_id: String, interval: Duration) -> JobStream<Self::Output>;

    fn ack(&mut self, worker_id: String, job_id: String) -> StorageResult<()>;

    fn retry(&mut self, worker_id: String, job_id: String) -> StorageResult<()>;

    fn keep_alive(&mut self, worker_id: String) -> StorageResult<()>;

    fn kill(&mut self, worker_id: String, job_id: String) -> StorageResult<()>;

    fn update_by_id(&self, job_id: String, job: &JobRequest<Self::Output>) -> StorageResult<()>;

    fn heartbeat(&mut self, pulse: WorkerPulse) -> StorageResult<bool>;

    fn reschedule(&mut self, job: &JobRequest<Self::Output>, wait: Duration) -> StorageResult<()>;

    fn reenqueue_active(&mut self, _job_ids: Vec<String>) -> StorageResult<()> {
        let fut = async { Ok(()) };
        Box::pin(fut)
    }
}

pub trait StorageJobExt<Output>: Storage<Output = Output>
where
    Self: Sized,
{
    //fn list_workers(&mut self) -> StorageResult<Output>;
    fn list_jobs(&mut self, status: &JobState, page: i32)
        -> StorageResult<Vec<JobRequest<Output>>>;
}
