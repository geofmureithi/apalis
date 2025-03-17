use std::{any::type_name, future::Future};

use futures::Stream;
use serde::{Deserialize, Serialize};

use crate::{
    codec::Codec,
    poller::Poller,
    request::State,
    worker::{Context, Worker},
};

/// A backend represents a task source
/// Both [`Storage`] and [`MessageQueue`] need to implement it for workers to be able to consume tasks
///
/// [`Storage`]: crate::storage::Storage
/// [`MessageQueue`]: crate::mq::MessageQueue
pub trait Backend<Req> {
    /// The stream to be produced by the backend
    type Stream: Stream<Item = Result<Option<Req>, crate::error::Error>>;

    /// Returns the final decoration of layers
    type Layer;

    /// Specifies the codec type used by the backend
    type Codec: Codec;

    /// Returns a poller that is ready for streaming
    fn poll(self, worker: &Worker<Context>) -> Poller<Self::Stream, Self::Layer>;
}

/// Represents functionality that allows reading of jobs and stats from a backend
/// Some backends esp MessageQueues may not currently implement this
pub trait BackendExpose<T>
where
    Self: Sized,
{
    /// The request type being handled by the backend
    type Request;
    /// The error returned during reading jobs and stats
    type Error;
    /// List all Workers that are working on a backend
    fn list_workers(
        &self,
    ) -> impl Future<Output = Result<Vec<Worker<WorkerState>>, Self::Error>> + Send;

    /// Returns the counts of jobs in different states
    fn stats(&self) -> impl Future<Output = Result<Stat, Self::Error>> + Send;

    /// Fetch jobs persisted in a backend
    fn list_jobs(
        &self,
        status: &State,
        page: i32,
    ) -> impl Future<Output = Result<Vec<Self::Request>, Self::Error>> + Send;
}

/// Represents the current statistics of a backend
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Stat {
    /// Represents pending tasks
    pub pending: usize,
    /// Represents running tasks
    pub running: usize,
    /// Represents dead tasks
    pub dead: usize,
    /// Represents failed tasks
    pub failed: usize,
    /// Represents successful tasks
    pub success: usize,
}

/// A serializable version of a worker's state.
#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerState {
    /// Type of task being consumed by the worker, useful for display and filtering
    pub r#type: String,
    /// The type of job stream
    pub source: String,
    // TODO: // The layers that were loaded for worker.
    // TODO: // pub layers: Vec<Layer>,
    // TODO: // last_seen: Timestamp,
}
impl WorkerState {
    /// Build a new state
    pub fn new<S>(r#type: String) -> Self {
        Self {
            r#type,
            source: type_name::<S>().to_string(),
        }
    }
}
