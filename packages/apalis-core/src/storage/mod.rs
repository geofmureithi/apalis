use std::time::Duration;

use futures::{stream::BoxStream, Future};

use crate::{request::Request, Backend};

/// The result of sa stream produced by a [Storage]
pub type StorageStream<T, E> = BoxStream<'static, Result<Option<Request<T>>, E>>;

/// Represents a [Storage] that can persist a request.
/// The underlying type must implement [Job]
pub trait Storage: Backend<Request<Self::Job>> {
    /// The type of job that can be persisted
    type Job: Job;

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
    fn len(&self) -> impl Future<Output = Result<i64, Self::Error>> + Send;

    /// Fetch a job given an id
    fn fetch_by_id(
        &self,
        job_id: &Self::Identifier,
    ) -> impl Future<Output = Result<Option<Request<Self::Job>>, Self::Error>> + Send;

    /// Update a job details
    fn update(
        &self,
        job: Request<Self::Job>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Reschedule a job
    fn reschedule(
        &mut self,
        job: Request<Self::Job>,
        wait: Duration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Returns true if there is no jobs in the storage
    fn is_empty(&self) -> impl Future<Output = Result<bool, Self::Error>> + Send;
}

/// Trait representing a job.
///
///
/// # Example
/// ```rust
/// # use apalis_core::storage::Job;
/// # struct Email;
/// impl Job for Email {
///     const NAME: &'static str = "apalis::Email";
/// }
/// ```
pub trait Job {
    /// Represents the name for job.
    const NAME: &'static str;
}
