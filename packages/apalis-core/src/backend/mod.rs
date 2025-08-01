//! Represents a task source that provides internal middleware and can be polled
//!
//! Also includes helper traits
use std::{fmt::Debug, future::Future, marker::PhantomData, time::Duration};

use futures_util::{
    future::{pending, ready},
    stream::{self, BoxStream},
    FutureExt, Stream, StreamExt, TryFutureExt,
};
use tower::layer::util::Identity;

use crate::{
    backend::codec::Encoder,
    error::BoxDynError,
    request::{task_id::TaskId, Parts, Request},
    worker::context::WorkerContext,
};

pub mod codec;
pub mod memory;
pub mod pipe;
pub mod shared;

/// A backend represents a task source
pub trait Backend<Args, Ctx> {
    type Error;
    type Stream: Stream<Item = Result<Option<Request<Args, Ctx>>, Self::Error>>;
    type Beat: Stream<Item = Result<(), Self::Error>>;
    type Layer;
    type Sink;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat;
    fn middleware(&self) -> Self::Layer;
    fn sink(&self) -> Self::Sink;
    fn poll(self, worker: &WorkerContext) -> Self::Stream;
}
/// Represents a stream for T.
pub type RequestStream<T, E = BoxDynError> = BoxStream<'static, Result<Option<T>, E>>;

pub trait TaskSink<Args>: Unpin + Send {
    type Codec;
    type Compact: Send;

    type Error;

    type Context: Default + Send + Clone;
    type Timestamp;

    fn push(
        &mut self,
        task: Args,
    ) -> impl Future<Output = Result<Parts<Self::Context>, Self::Error>> + Send
    where
        Self::Context: Default,
        Self::Codec: Encoder<Args, Compact = Self::Compact>,
    {
        self.push_request(Request::new(task))
    }

    fn push_request(
        &mut self,
        req: Request<Args, Self::Context>,
    ) -> impl Future<Output = Result<Parts<Self::Context>, Self::Error>> + Send
    where
        Self::Codec: Encoder<Args, Compact = Self::Compact>,
    {
        let res = match Self::Codec::encode(&req.args) {
            Ok(r) => r,
            Err(_) => todo!(),
        };
        let req = Request::new(res);
        self.push_raw_request(req)
    }

    fn push_raw_request(
        &mut self,
        req: Request<Self::Compact, Self::Context>,
    ) -> impl Future<Output = Result<Parts<Self::Context>, Self::Error>> + Send;

    fn schedule(
        &mut self,
        task: Args,
        delay: Duration,
    ) -> impl Future<Output = Result<Parts<Self::Context>, Self::Error>> + Send
    where
        Self::Context: Default,
    {
        self.schedule_request(Request::new(task), delay)
    }

    fn schedule_request(
        &mut self,
        request: Request<Args, Self::Context>,
        delay: Duration,
    ) -> impl Future<Output = Result<Parts<Self::Context>, Self::Error>> + Send {
        Box::pin(async { todo!() })
    }

    fn schedule_raw_request(
        &mut self,
        request: Request<Self::Compact, Self::Context>,
        delay: Duration,
    ) -> impl Future<Output = Result<Parts<Self::Context>, Self::Error>> + Send {
        Box::pin(async { todo!() })
    }
}

pub trait FetchById<T, Context>: Backend<T, Context> {
    fn fetch_by_id(
        &mut self,
        task_id: &TaskId,
    ) -> impl Future<Output = Result<Option<Request<T, Context>>, Self::Error>> + Send;
}

pub trait Update<T, Context>: Backend<T, Context> {
    fn update(
        &mut self,
        task: Request<T, Context>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait Reschedule<T, Context>: Backend<T, Context> {
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

pub trait ConsumeNext<T, Context>: Backend<T, Context> {
    fn consume_next(
        &mut self,
    ) -> impl Future<Output = Result<Option<Request<T, Context>>, Self::Error>> + Send;
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
    fn fetch_many(&mut self) -> RequestStream<Request<Self::Compact, Context>>;
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
    ) -> impl Future<Output = Result<Vec<Request<Args, Ctx>>, Self::Error>> + Send;
}
