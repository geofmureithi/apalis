/// Represents a worker that is ready to consume jobs
pub mod ready;
use crate::executor::Executor;
use async_trait::async_trait;
use futures::Future;
use graceful_shutdown::Shutdown;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fmt::{self, Display};
use std::time::Duration;
use thiserror::Error;

/// A worker name wrapper usually used by Worker builder
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerId {
    name: String,
}

impl Display for WorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl WorkerId {
    /// Build a new worker ref
    pub fn new<T: AsRef<str>>(name: T) -> Self {
        Self {
            name: name.as_ref().to_string(),
        }
    }
    /// Get the name of the worker
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Possible errors that can occur when starting a worker.
#[derive(Error, Debug)]
pub enum WorkerError {
    /// An error occurred while processing a job.
    #[error("Failed to process job: {0}")]
    JobProcessingError(String),
    /// An error occurred in the worker's service.
    #[error("Service error: {0}")]
    ServiceError(String),
    /// An error occurred while trying to start the worker.
    #[error("Failed to start worker: {0}")]
    StartError(String),
}
/// The `Worker` trait represents a type that can execute jobs. It is used
/// to define workers that can be managed by the `Monitor`.
///
/// Each `Worker` implementation must define a `start` method that takes a
/// `WorkerContext` and returns a `Result` indicating whether the worker
/// was able to execute its jobs successfully or not.
#[async_trait]
pub trait Worker<Job>: Sized {
    /// The [tower] service type that this worker will use to execute jobs.
    type Service;

    /// The source type that this worker will use to receive jobs.
    type Source;

    /// A worker must be identifiable and unique
    fn id(&self) -> WorkerId;

    /// Starts the worker, taking ownership of `self` and the provided `ctx`.
    ///
    /// This method should run indefinitely or until it returns an error.
    /// If an error occurs, it should return a `WorkerError` describing
    /// the reason for the failure.
    async fn start<E: Executor + Send>(self, ctx: WorkerContext<E>) -> Result<(), WorkerError>;
}

/// Stores the Workers context
pub struct WorkerContext<E: Executor> {
    pub(crate) shutdown: Shutdown,
    pub(crate) executor: E,
    pub(crate) worker_id: WorkerId
}

impl<E: Executor> fmt::Debug for WorkerContext<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerContext")
            .field("shutdown", &["Shutdown handle"])
            .field("worker_id", &self.worker_id)
            .finish()
    }
}

impl<E: Executor + Send> WorkerContext<E> {
    /// Get the Worker ID
    pub fn id(&self) -> WorkerId {
        self.worker_id.clone()
    }
    /// Allows spawning of futures that will be gracefully shutdown by the worker
    pub fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        self.executor.spawn(self.shutdown.graceful(future));
    }

    /// Calling this function triggers shutting down the worker
    pub fn shutdown(&self) {}
}

/// A worker can have heartbeats to keep alive or enqueue new jobs
#[async_trait::async_trait]
pub trait HeartBeat {
    /// The future of a single beat
    async fn heart_beat(&mut self);
    /// The interval for each beat to be called
    fn interval(&self) -> Duration;
}
