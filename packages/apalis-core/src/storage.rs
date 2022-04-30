use chrono::Duration;
use futures::future::BoxFuture;
use serde::Serialize;

use crate::{error::StorageError, queue::Heartbeat, request::JobRequest};

pub type StorageResult<I> = BoxFuture<'static, Result<I, StorageError>>;

/// Represents a [Storage] that can be passed to a [crate::builder::QueueBuilder]
pub trait Storage: Clone {
    type Output: Serialize;

    /// Pushes a job to a storage
    /// TODO: return id
    fn push(&mut self, job: Self::Output) -> StorageResult<()>;

    /// Get the next job in the queue,
    fn consume(&mut self) -> StorageResult<Option<JobRequest<Self::Output>>>;

    fn len(&self) -> i64 {
        0
    }

    fn ack(&mut self, job_id: String) -> StorageResult<()> {
        let fut = async { Ok(()) };
        Box::pin(fut)
    }

    fn retry(&mut self, job_id: String) -> StorageResult<()> {
        let fut = async { Ok(()) };
        Box::pin(fut)
    }

    fn heartbeat(&mut self, beat: Heartbeat) -> StorageResult<bool> {
        let fut = async { Ok(true) };
        Box::pin(fut)
    }

    fn kill(&mut self, job_id: String) -> StorageResult<()> {
        let fut = async { Ok(()) };
        Box::pin(fut)
    }

    fn reschedule(&mut self, job_id: String, wait: Duration) -> StorageResult<()> {
        let fut = async { Ok(()) };
        Box::pin(fut)
    }
}
