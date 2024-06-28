use std::time::Duration;

use futures::{stream::BoxStream, Future};

use crate::{request::Request, Backend};

/// The result of sa stream produced by a [Storage]
pub type StorageStream<T, E> = BoxStream<'static, Result<Option<Request<T>>, E>>;

/// Represents a [Storage] that can persist a request.
/// The underlying type must implement [Job]
pub trait Storage: Backend<Request<Self::Job>> {
    /// The type of job that can be persisted
    type Job;

    /// The error produced by the storage
    type Error;

    /// Jobs must have Ids.
    type Identifier;

    /// Pushes a job to a storage
    fn push(
        &mut self,
        job: Self::Job,
    ) -> impl Future<Output = Result<Self::Identifier, Self::Error>> + Send;

    /// Push a job into the scheduled set
    fn schedule(
        &mut self,
        job: Self::Job,
        on: i64,
    ) -> impl Future<Output = Result<Self::Identifier, Self::Error>> + Send;

    /// Return the number of pending jobs from the queue
    fn len(&mut self) -> impl Future<Output = Result<i64, Self::Error>> + Send;

    /// Fetch a job given an id
    fn fetch_by_id(
        &mut self,
        job_id: &Self::Identifier,
    ) -> impl Future<Output = Result<Option<Request<Self::Job>>, Self::Error>> + Send;

    /// Update a job details
    fn update(
        &mut self,
        job: Request<Self::Job>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Reschedule a job
    fn reschedule(
        &mut self,
        job: Request<Self::Job>,
        wait: Duration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Returns true if there is no jobs in the storage
    fn is_empty(&mut self) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    /// Vacuum the storage, removes done and killed jobs
    fn vacuum(&mut self) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}
