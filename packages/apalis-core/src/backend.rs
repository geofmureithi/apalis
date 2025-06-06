use std::{future::Future, time::Duration};

use futures::Stream;

use crate::{
    request::{Parts, Request, RequestStream},
    task::task_id::TaskId,
    worker::{WorkerContext, WorkerId},
};

/// A backend represents a task source
/// Both [`Storage`] and [`MessageQueue`] need to implement it for workers to be able to consume tasks
///
/// [`Storage`]: crate::storage::Storage
/// [`MessageQueue`]: crate::mq::MessageQueue
pub trait Backend<Req> {
    type Error;
    type Stream: Stream<Item = Result<Option<Req>, Self::Error>>;
    type Beat: Stream<Item = Result<(), Self::Error>>;
    type Layer;
    type Codec;
    fn heartbeat(&self) -> Self::Beat;
    fn middleware(&self) -> Self::Layer;
    fn poll(self, worker: &WorkerContext) -> Self::Stream;
}

pub trait Push<T, Context>: Backend<Request<T, Context>> {
    type Compact;
    fn push(&mut self, task: T) -> impl Future<Output = Result<Parts<Context>, Self::Error>> + Send
    where
        Context: Default,
    {
        self.push_request(Request::new(task))
    }

    fn push_request(
        &mut self,
        req: Request<T, Context>,
    ) -> impl Future<Output = Result<Parts<Context>, Self::Error>> + Send;

    fn push_raw_request(
        &mut self,
        req: Request<Self::Compact, Context>,
    ) -> impl Future<Output = Result<Parts<Context>, Self::Error>> + Send;
}

pub trait Schedule<T, Context>: Push<T, Context> {
    type Timestamp;
    fn schedule(
        &mut self,
        task: T,
        on: Self::Timestamp,
    ) -> impl Future<Output = Result<Parts<Context>, Self::Error>> + Send
    where
        Context: Default,
    {
        self.schedule_request(Request::new(task), on)
    }

    fn schedule_request(
        &mut self,
        request: Request<T, Context>,
        on: Self::Timestamp,
    ) -> impl Future<Output = Result<Parts<Context>, Self::Error>> + Send;

    fn schedule_raw_request(
        &mut self,
        request: Request<Self::Compact, Context>,
        on: Self::Timestamp,
    ) -> impl Future<Output = Result<Parts<Context>, Self::Error>> + Send;
}

pub trait Metric<Output> {
    type Error;
    fn metric(&mut self) -> impl Future<Output = Result<Output, Self::Error>> + Send;
}

pub trait FetchById<T, Context>: Backend<Request<T, Context>> {
    fn fetch_by_id(
        &mut self,
        task_id: &TaskId,
    ) -> impl Future<Output = Result<Option<Request<T, Context>>, Self::Error>> + Send;
}

pub trait Update<T, Context>: Backend<Request<T, Context>> {
    fn update(
        &mut self,
        task: Request<T, Context>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait Reschedule<T, Context>: Backend<Request<T, Context>> {
    fn reschedule(
        &mut self,
        task: Request<T, Context>,
        wait: Duration,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait Vacuum {
    type Error;
    fn vacuum(&mut self) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}

pub trait ConsumeNext<T, Context>: Backend<Request<T, Context>> {
    fn consume_next(
        &mut self,
    ) -> impl Future<Output = Result<Option<Request<T, Context>>, Self::Error>> + Send;
}

pub trait ConsumeBatch<T, Context>: Backend<Request<T, Context>> {
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
    fn fetch_many(&mut self) -> RequestStream<Request<Self::Compact, Context>>;
}

pub trait ResumeById<T, Context>: Backend<Request<T, Context>> {
    type Id;

    fn resume_by_id(
        &mut self,
        id: Self::Id,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;
}

pub trait ResumeAbandoned<T, Context>: Backend<Request<T, Context>> {
    fn resume_abandoned(&mut self) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}

pub trait RegisterWorker<T, Context>: Backend<Request<T, Context>> {
    fn register_worker(
        &mut self,
        worker_id: WorkerId,
    ) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}

pub trait Notify<Event> {
    type Error;
    fn notify(&mut self, ev: Event) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait ListWorkers<Req>: Backend<Req> {
    type Worker;
    fn list_workers(&self) -> impl Future<Output = Result<Vec<Self::Worker>, Self::Error>> + Send;
}

pub trait ListTasks<Req>: Backend<Req> {
    type Filter;
    fn list_tasks(
        &self,
        filter: &Self::Filter,
    ) -> impl Future<Output = Result<Vec<Req>, Self::Error>> + Send;
}
