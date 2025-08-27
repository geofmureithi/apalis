//! Represents a task source that provides internal middleware and can be polled
//!
//! Also includes helper traits
use std::{future::Future, time::Duration};

use futures_sink::Sink;
use futures_util::{
    stream::{self, BoxStream},
    Stream,
};

use crate::{
    backend::codec::Codec,
    error::BoxDynError,
    task::{task_id::TaskId, Task},
    worker::context::WorkerContext,
};

pub mod codec;
pub mod custom;
pub mod memory;
pub mod pipe;
pub mod shared;

/// A backend represents a task source
pub trait Backend<Args> {
    type IdType: Clone;
    type Meta: Default;
    type Error;
    type Stream: Stream<Item = Result<Option<Task<Args, Self::Meta, Self::IdType>>, Self::Error>>;
    type Beat: Stream<Item = Result<(), Self::Error>>;
    type Layer;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat;
    fn middleware(&self) -> Self::Layer;
    fn poll(self, worker: &WorkerContext) -> Self::Stream;
}

pub trait BackendWithCodec {
    type Codec;
    type Compact;
}

/// Represents a stream for T.
pub type TaskStream<T, E = BoxDynError> = BoxStream<'static, Result<Option<T>, E>>;

pub trait TaskSink<Args>: Backend<Args> {
    fn push(&mut self, task: Args) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn push_bulk(
        &mut self,
        tasks: Vec<Args>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn push_stream(
        &mut self,
        tasks: impl Stream<Item = Args> + Unpin + Send,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn push_raw(
        &mut self,
        task: Task<Args, Self::Meta, Self::IdType>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

impl<Args, S, E> TaskSink<Args> for S
where
    S: Sink<Task<Args, Self::Meta, Self::IdType>, Error = E>
        + Unpin
        + Backend<Args, Error = E>
        + Send,
    Args: Send,
    S::Meta: Send + Default,
    S::IdType: Send + 'static,
    E: Send
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

    async fn push_raw(
        &mut self,
        task: Task<Args, Self::Meta, Self::IdType>,
    ) -> Result<(), Self::Error> {
        use futures_util::SinkExt;
        self.send(task).await
    }
}

pub trait FetchById<Args>: Backend<Args> {
    fn fetch_by_id(
        &mut self,
        task_id: &TaskId,
    ) -> impl Future<Output = Result<Option<Task<Args, Self::Meta>>, Self::Error>> + Send;
}

pub trait Update<Args>: Backend<Args> {
    fn update(
        &mut self,
        task: Task<Args, Self::Meta>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait Reschedule<Args>: Backend<Args> {
    fn reschedule(
        &mut self,
        task: Task<Args, Self::Meta>,
        wait: Duration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait Vacuum {
    type Error;
    fn vacuum(&mut self) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}

pub trait FetchBatch<T> {
    type Id;
    type Error;
    type Stream: Stream<Item = Result<Option<T>, Self::Error>>;
    fn fetch_batch(&mut self, ids: &[Self::Id]) -> Self::Stream;
}

pub trait FetchAll<Meta> {
    type Compact;
    fn fetch_many(&mut self) -> TaskStream<Task<Self::Compact, Meta>>;
}

pub trait ResumeById<T>: Backend<T> {
    type Id;

    fn resume_by_id(
        &mut self,
        id: Self::Id,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;
}

pub trait ResumeAbandoned<T, Context>: Backend<T> {
    fn resume_abandoned(&mut self) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}

pub trait RegisterWorker<T, Context>: Backend<T> {
    fn register_worker(
        &mut self,
        worker_id: String,
    ) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}

pub trait Metric<Output> {
    type Error;
    fn metric(&mut self) -> impl Future<Output = Result<Output, Self::Error>> + Send;
}

pub trait ListWorkers<Args>: Backend<Args> {
    type Worker;
    fn list_workers(&self) -> impl Future<Output = Result<Vec<Self::Worker>, Self::Error>> + Send;
}

pub trait ListTasks<Args>: Backend<Args> {
    type Filter;
    fn list_tasks(
        &self,
        filter: &Self::Filter,
    ) -> impl Future<Output = Result<Vec<Task<Args, Self::Meta, Self::IdType>>, Self::Error>> + Send;
}

use crate::task::status::Status;
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone)]
pub struct TaskResult<T> {
    pub task_id: TaskId,
    pub status: Status,
    pub result: Result<T, String>,
}

pub trait WaitForCompletion<T, Args>: Backend<Args> {
    type ResultStream: Stream<Item = Result<TaskResult<T>, Self::Error>> + Send + 'static;

    /// Wait for multiple tasks to complete, yielding results as they become available
    fn wait_for(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>>,
    ) -> Self::ResultStream;

    fn wait_for_single(&self, task_id: TaskId<Self::IdType>) -> Self::ResultStream {
        self.wait_for(std::iter::once(task_id))
    }

    /// Check current status of tasks without waiting
    async fn check_status(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>>,
    ) -> Result<Vec<TaskResult<T>>, Self::Error>;
}
