mod error;
#[cfg(feature = "worker")]
mod streams;
#[cfg(feature = "worker")]
mod worker;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::{
    job::JobStream,
    job::{Job, JobStreamResult},
    request::JobRequest,
};

#[cfg(feature = "storage")]
pub use self::error::StorageError;

#[cfg(feature = "worker")]
pub use worker::StorageWorker;

/// Represents a Storage Result
pub type StorageResult<I> = Result<I, StorageError>;

/// Represents a [Storage] that can be passed to a [Builder]
///
/// [Builder]: crate::builder::WorkerBuilder
#[async_trait::async_trait]
pub trait Storage: Clone {
    /// The type of job that can be persisted
    type Output: Job;

    /// Pushes a job to a storage
    ///
    /// TODO: return id
    async fn push(&mut self, job: Self::Output) -> StorageResult<()>;

    /// Push a job into the scheduled set
    async fn schedule(&mut self, job: Self::Output, on: DateTime<Utc>) -> StorageResult<()>;

    /// Return the number of pending jobs from the queue
    async fn len(&self) -> StorageResult<i64>;

    /// Fetch a job given an id
    async fn fetch_by_id(&self, job_id: String) -> StorageResult<Option<JobRequest<Self::Output>>>;

    /// Get the stream of jobs
    fn consume(&mut self, worker_id: String, interval: Duration) -> JobStreamResult<Self::Output>;

    /// Acknowledge a job which returns [JobResult::Success]
    async fn ack(&mut self, worker_id: String, job_id: String) -> StorageResult<()>;

    /// Retry a job which returns [JobResult::Retry]
    async fn retry(&mut self, worker_id: String, job_id: String) -> StorageResult<()>;

    /// Called by a Worker to keep the storage alive and prevent jobs from being deemed as orphaned
    async fn keep_alive<Service>(&mut self, worker_id: String) -> StorageResult<()>;

    /// Kill a job that returns [JobResult::Kill]
    async fn kill(&mut self, worker_id: String, job_id: String) -> StorageResult<()>;

    /// Update a job details
    async fn update_by_id(
        &self,
        job_id: String,
        job: &JobRequest<Self::Output>,
    ) -> StorageResult<()>;

    /// Used for scheduling jobs
    async fn heartbeat(&mut self, pulse: StorageWorkerPulse) -> StorageResult<bool>;

    /// Kill a job that returns [JobResult::Reschedule]
    /// [`JobResult::Reschedule`]: crate::response::JobResult
    async fn reschedule(
        &mut self,
        job: &JobRequest<Self::Output>,
        wait: Duration,
    ) -> StorageResult<()>;

    /// Used to recover jobs when a Worker shuts down.
    async fn reenqueue_active(&mut self, _job_ids: Vec<String>) -> StorageResult<()> {
        Ok(())
    }

    /// This method is not implemented yet but its self explanatory
    #[doc(hidden)]
    async fn is_empty(&self) -> StorageResult<bool> {
        unimplemented!()
    }
}

/// Each [Worker] sends heartbeat messages to storage
#[non_exhaustive]
#[derive(Debug, Clone, Hash, PartialEq, Eq)]

pub enum StorageWorkerPulse {
    /// Push scheduled jobs into the active set
    EnqueueScheduled {
        /// the count of jobs to be scheduled
        count: i32,
    },
    /// Resque any orphaned jobs
    RenqueueOrpharned {
        /// the count of orphaned jobs
        count: i32,
    },
}

impl<S> JobStream for S
where
    S: Storage,
{
    type Job = S::Output;

    /// Consume the stream of jobs from Storage
    fn stream(&mut self, worker_id: String, interval: Duration) -> JobStreamResult<S::Output> {
        self.consume(worker_id, interval)
    }
}
