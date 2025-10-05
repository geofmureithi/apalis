//! Core traits for interacting with backends
//!
//! The core traits and types for backends, responsible for providing sources of tasks, handling their lifecycle, and exposing middleware for internal processing.
//! The traits here abstract over different backend implementations, allowing for extensibility and interoperability.
//!
//! # Overview
//! - [`Backend`]: The primary trait representing a task source, defining methods for polling tasks, heartbeats, and middleware.
//! - [`TaskSink`]: An extension trait for backends that support pushing tasks.
//! - [`FetchById`], [`Update`], [`Reschedule`]: Additional traits for managing tasks.
//! - [`Vacuum`], [`ResumeById`], [`ResumeAbandoned`]: Traits for backend maintenance and task recovery.
//! - [`RegisterWorker`], [`ListWorkers`], [`ListTasks`]: Traits for worker management and task listing.
//! - [`WaitForCompletion`]: A trait for waiting on task completion and checking their status.
//!
//!
//! ## Default Implementations
//!
//! The module includes several default backend implementations, such as:
//! - [`MemoryStorage`](memory::MemoryStorage): An in-memory backend for testing and lightweight use cases
//! - [`Pipe`](pipe::Pipe): A simple pipe-based backend for inter-thread communication
//! - [`CustomBackend`](custom::CustomBackend): A flexible backend allowing custom functions for task management
use std::{future::Future, time::Duration};

use futures_sink::Sink;
use futures_util::{
    Stream,
    stream::{self, BoxStream},
};

use crate::{
    backend::codec::Codec,
    error::BoxDynError,
    task::{Task, status::Status, task_id::TaskId},
    worker::context::WorkerContext,
};

pub mod codec;
pub mod custom;
pub mod pipe;
pub mod poll_strategy;
pub mod shared;

mod expose;
mod impls;

pub use expose::*;

pub use impls::guide;

/// In-memory backend based on channels
pub mod memory {
    pub use crate::backend::impls::memory::*;
}

/// File based Backend using JSON
#[cfg(feature = "json")]
pub mod json {
    pub use crate::backend::impls::json::*;
}

/// The `Backend` trait defines how workers get and manage tasks from a backend.
///
/// In other languages, this might be called a "Queue", "Broker", etc.
pub trait Backend {
    /// The type of arguments the backend handles.
    type Args;
    /// The type used to uniquely identify tasks.
    type IdType: Clone;
    /// Context associated with each task.
    type Context: Default;
    /// The error type returned by backend operations
    type Error;
    /// The codec used for serialization/deserialization of tasks.
    type Codec: Codec<Self::Args, Compact = Self::Compact>;

    /// The compact representation of task arguments.
    type Compact;

    /// A stream of tasks provided by the backend.
    type Stream: Stream<
        Item = Result<Option<Task<Self::Args, Self::Context, Self::IdType>>, Self::Error>,
    >;
    /// A stream representing heartbeat signals.
    type Beat: Stream<Item = Result<(), Self::Error>>;
    /// The type representing backend middleware layer.
    type Layer;

    /// Returns a heartbeat stream for the given worker.
    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat;
    /// Returns the backend's middleware layer.
    fn middleware(&self) -> Self::Layer;
    /// Polls the backend for tasks for the given worker.
    fn poll(self, worker: &WorkerContext) -> Self::Stream;
}

/// Represents a stream for T.
pub type TaskStream<T, E = BoxDynError> = BoxStream<'static, Result<Option<T>, E>>;

/// Extends Backend to allow pushing tasks into the backend
pub trait TaskSink<Args>: Backend<Args = Args> {
    /// Allows pushing a single task into the backend
    fn push(&mut self, task: Args) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Allows pushing multiple tasks into the backend in bulk
    fn push_bulk(
        &mut self,
        tasks: Vec<Args>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Allows pushing tasks from a stream into the backend
    fn push_stream(
        &mut self,
        tasks: impl Stream<Item = Args> + Unpin + Send,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Allows pushing a fully constructed task into the backend
    fn push_task(
        &mut self,
        task: Task<Args, Self::Context, Self::IdType>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

impl<Args, S, E> TaskSink<Args> for S
where
    S: Sink<Task<Args, Self::Context, Self::IdType>, Error = E>
        + Unpin
        + Backend<Args = Args, Error = E>
        + Send,
    Args: Send,
    S::Context: Send + Default,
    S::IdType: Send + 'static,
    E: Send,
{
    async fn push(&mut self, task: Args) -> Result<(), Self::Error> {
        use futures_util::SinkExt;
        self.send(Task::new(task)).await
    }

    async fn push_bulk(&mut self, tasks: Vec<Args>) -> Result<(), Self::Error> {
        use futures_util::SinkExt;
        self.send_all(&mut stream::iter(
            tasks
                .into_iter()
                .map(Task::new)
                .map(Result::Ok)
                .collect::<Vec<_>>(),
        ))
        .await
    }

    async fn push_stream(
        &mut self,
        tasks: impl Stream<Item = Args> + Unpin + Send,
    ) -> Result<(), Self::Error> {
        use futures_util::SinkExt;
        use futures_util::StreamExt;
        self.send_all(&mut tasks.map(Task::new).map(Result::Ok))
            .await
    }

    async fn push_task(
        &mut self,
        task: Task<Args, Self::Context, Self::IdType>,
    ) -> Result<(), Self::Error> {
        use futures_util::SinkExt;
        self.send(task).await
    }
}

/// Allows fetching a task by its ID
pub trait FetchById: Backend {
    /// Fetch a task by its unique identifier
    fn fetch_by_id(
        &mut self,
        task_id: &TaskId<Self::IdType>,
    ) -> impl Future<
        Output = Result<Option<Task<Self::Args, Self::Context, Self::IdType>>, Self::Error>,
    > + Send;
}

/// Allows updating an existing task
pub trait Update: Backend {
    /// Update the given task
    fn update(
        &mut self,
        task: Task<Self::Args, Self::Context, Self::IdType>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Allows rescheduling a task for later execution
pub trait Reschedule: Backend {
    /// Reschedule the task after a specified duration
    fn reschedule(
        &mut self,
        task: Task<Self::Args, Self::Context, Self::IdType>,
        wait: Duration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Allows cleaning up resources in the backend
pub trait Vacuum: Backend {
    /// Cleans up resources and returns the number of items vacuumed
    fn vacuum(&mut self) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}

/// Allows resuming a task by its ID
pub trait ResumeById: Backend {
    /// Resume a task by its ID
    fn resume_by_id(
        &mut self,
        id: TaskId<Self::IdType>,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;
}

/// Allows fetching multiple tasks by their IDs
pub trait ResumeAbandoned: Backend {
    /// Resume all abandoned tasks
    fn resume_abandoned(&mut self) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}

/// Allows registering a worker with the backend
pub trait RegisterWorker: Backend {
    /// Registers a worker
    fn register_worker(
        &mut self,
        worker_id: String,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Represents the result of a task execution
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone)]
pub struct TaskResult<T> {
    task_id: TaskId,
    status: Status,
    result: Result<T, String>,
}

impl<T> TaskResult<T> {
    /// Create a new TaskResult
    pub fn new(task_id: TaskId, status: Status, result: Result<T, String>) -> Self {
        Self {
            task_id,
            status,
            result,
        }
    }
    /// Get the ID of the task
    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    /// Get the status of the task
    pub fn status(&self) -> &Status {
        &self.status
    }

    /// Get the result of the task
    pub fn result(&self) -> &Result<T, String> {
        &self.result
    }

    /// Take the result of the task
    pub fn take(self) -> Result<T, String> {
        self.result
    }
}

/// Allows waiting for tasks to complete and checking their status
pub trait WaitForCompletion<T>: Backend {
    /// The result stream type yielding task results
    type ResultStream: Stream<Item = Result<TaskResult<T>, Self::Error>> + Send + 'static;

    /// Wait for multiple tasks to complete, yielding results as they become available
    fn wait_for(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>>,
    ) -> Self::ResultStream;

    /// Wait for a single task to complete, yielding its result
    fn wait_for_single(&self, task_id: TaskId<Self::IdType>) -> Self::ResultStream {
        self.wait_for(std::iter::once(task_id))
    }

    /// Check current status of tasks without waiting
    fn check_status(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>> + Send,
    ) -> impl Future<Output = Result<Vec<TaskResult<T>>, Self::Error>> + Send;
}
