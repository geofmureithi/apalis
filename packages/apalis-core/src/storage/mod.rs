use std::time::Duration;

use futures::Future;

use crate::{
    backend::Backend,
    request::{Parts, Request},
    task::task_id::TaskId,
};

/// Represents a [Storage] that can persist a request.
pub trait Storage: Backend<Request<Self::Job, Self::Context>> {
    /// The type of job that can be persisted
    type Job;

    /// The error produced by the storage
    type Error;

    /// This is the type that storages store as the metadata related to a job
    type Context: Default;

    /// The format that the storage persists the jobs usually `Vec<u8>`
    type Compact;

    /// Pushes a job to a storage
    fn push(
        &mut self,
        job: Self::Job,
    ) -> impl Future<Output = Result<Parts<Self::Context>, Self::Error>> + Send {
        self.push_request(Request::new(job))
    }

    /// Pushes a constructed request to a storage
    fn push_request(
        &mut self,
        req: Request<Self::Job, Self::Context>,
    ) -> impl Future<Output = Result<Parts<Self::Context>, Self::Error>> + Send;

    /// Pushes a constructed request to a storage
    fn push_raw_request(
        &mut self,
        req: Request<Self::Compact, Self::Context>,
    ) -> impl Future<Output = Result<Parts<Self::Context>, Self::Error>> + Send;

    /// Push a job with defaults into the scheduled set
    fn schedule(
        &mut self,
        job: Self::Job,
        on: i64,
    ) -> impl Future<Output = Result<Parts<Self::Context>, Self::Error>> + Send {
        self.schedule_request(Request::new(job), on)
    }

    /// Push a request into the scheduled set
    fn schedule_request(
        &mut self,
        request: Request<Self::Job, Self::Context>,
        on: i64,
    ) -> impl Future<Output = Result<Parts<Self::Context>, Self::Error>> + Send;

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
