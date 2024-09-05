use std::time::Duration;

use futures::Future;

use crate::{request::Request, task::task_id::TaskId};

/// Represents a [Storage] that can persist a request.
pub trait Storage {
    /// The type of job that can be persisted
    type Job;

    /// The error produced by the storage
    type Error;

    /// This is the type that storages store as the metadata related to a job
    type Context: Default;

    /// Pushes a job to a storage
    fn push(
        &mut self,
        job: Self::Job,
    ) -> impl Future<Output = Result<Self::Context, Self::Error>> + Send {
        self.push_raw(Request::new(job))
    }

    /// Pushes a raw request to a storage
    fn push_raw(
        &mut self,
        req: Request<Self::Job, Self::Context>,
    ) -> impl Future<Output = Result<Self::Context, Self::Error>> + Send;

    /// Push a job into the scheduled set
    fn schedule(
        &mut self,
        job: Self::Job,
        on: i64,
    ) -> impl Future<Output = Result<Self::Context, Self::Error>> + Send;

    /// Return the number of pending jobs from the queue
    fn len(&mut self) -> impl Future<Output = Result<i64, Self::Error>> + Send;

    /// Fetch a job given an id
    fn fetch_by_id(
        &mut self,
        job_id: &TaskId,
    ) -> impl Future<Output = Result<Option<Request<Self::Job, Self::Context>>, Self::Error>> + Send;

    /// Update a job details
    fn update(
        &mut self,
        job: Request<Self::Job, Self::Context>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Reschedule a job
    fn reschedule(
        &mut self,
        job: Request<Self::Job, Self::Context>,
        wait: Duration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Returns true if there is no jobs in the storage
    fn is_empty(&mut self) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    /// Vacuum the storage, removes done and killed jobs
    fn vacuum(&mut self) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}
