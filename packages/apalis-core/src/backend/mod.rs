//! Represents a task source that provides internal middleware and can be polled
//!
//! Also includes helper traits
use std::{
    future::Future,
    task::{Context, Poll},
    time::Duration,
};

use futures_sink::Sink;
use futures_util::{
    stream::{self, BoxStream},
    Stream,
};

use crate::{
    backend::codec::Encoder,
    error::BoxDynError,
    task::{task_id::TaskId, Metadata, Task},
    worker::context::WorkerContext,
};

pub mod codec;
pub mod memory;
pub mod pipe;
pub mod shared;

/// A backend represents a task source
pub trait Backend<Args, Ctx> {
    type Error;
    type Stream: Stream<Item = Result<Option<Task<Args, Ctx>>, Self::Error>>;
    type Beat: Stream<Item = Result<(), Self::Error>>;
    type Layer;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat;
    fn middleware(&self) -> Self::Layer;
    fn poll(self, worker: &WorkerContext) -> Self::Stream;
}

pub trait BackendWithSink<Args, Ctx>: Backend<Args, Ctx> {
    type Sink: Sink<Task<Args, Ctx>>;

    #[must_use = "Sinks do nothing unless flushed"]
    fn sink(&mut self) -> Self::Sink;
}
/// Represents a stream for T.
pub type TaskStream<T, E = BoxDynError> = BoxStream<'static, Result<Option<T>, E>>;

pub trait TaskSink<Args, Context> {
    type Error;
    fn push(&mut self, task: Args) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn push_bulk(
        &mut self,
        tasks: Vec<Args>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn push_stream(
        &mut self,
        tasks: impl Stream<Item = Args> + Unpin + Send,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

impl<Args, Ctx: Default, S> TaskSink<Args, Ctx> for S
where
    S: Sink<Task<Args, Ctx>> + Unpin + Send,
    Args: Send,
    Ctx: Send,
    S::Error: Send,
{
    type Error = S::Error;
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
}

pub trait FetchById<T, Context>: Backend<T, Context> {
    fn fetch_by_id(
        &mut self,
        task_id: &TaskId,
    ) -> impl Future<Output = Result<Option<Task<T, Context>>, Self::Error>> + Send;
}

pub trait Update<T, Context>: Backend<T, Context> {
    fn update(
        &mut self,
        task: Task<T, Context>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait Reschedule<T, Context>: Backend<T, Context> {
    fn reschedule(
        &mut self,
        task: Task<T, Context>,
        wait: Duration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait Vacuum {
    type Error;
    fn vacuum(&mut self) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}

pub trait ConsumeNext<T, Context>: Backend<T, Context> {
    fn consume_next(
        &mut self,
    ) -> impl Future<Output = Result<Option<Task<T, Context>>, Self::Error>> + Send;
}

pub trait ConsumeBatch<T, Context>: Backend<T, Context> {
    fn consume_batch(&mut self) -> Self::Stream;
}

pub trait FetchBatch<T> {
    type Id;
    type Error;
    type Stream: Stream<Item = Result<Option<T>, Self::Error>>;
    fn fetch_batch(&mut self, ids: &[Self::Id]) -> Self::Stream;
}

pub trait FetchAll<Context> {
    type Compact;
    fn fetch_many(&mut self) -> TaskStream<Task<Self::Compact, Context>>;
}

pub trait ResumeById<T, Context>: Backend<T, Context> {
    type Id;

    fn resume_by_id(
        &mut self,
        id: Self::Id,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;
}

pub trait ResumeAbandoned<T, Context>: Backend<T, Context> {
    fn resume_abandoned(&mut self) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}

pub trait RegisterWorker<T, Context>: Backend<T, Context> {
    fn register_worker(
        &mut self,
        worker_id: String,
    ) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}

pub trait Metric<Output> {
    type Error;
    fn metric(&mut self) -> impl Future<Output = Result<Output, Self::Error>> + Send;
}

pub trait ListWorkers<Args, Ctx>: Backend<Args, Ctx> {
    type Worker;
    fn list_workers(&self) -> impl Future<Output = Result<Vec<Self::Worker>, Self::Error>> + Send;
}

pub trait ListTasks<Args, Ctx>: Backend<Args, Ctx> {
    type Filter;
    fn list_tasks(
        &self,
        filter: &Self::Filter,
    ) -> impl Future<Output = Result<Vec<Task<Args, Ctx>>, Self::Error>> + Send;
}
