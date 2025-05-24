use std::{future::Future, io};

use futures::Stream;

use crate::worker::WorkerContext;

/// A backend represents a task source
/// Both [`Storage`] and [`MessageQueue`] need to implement it for workers to be able to consume tasks
///
/// [`Storage`]: crate::storage::Storage
/// [`MessageQueue`]: crate::mq::MessageQueue
pub trait Backend<Req> {
    type Error: Send + Sync + std::error::Error + 'static;
    type Stream: Stream<Item = Result<Option<Req>, Self::Error>>;
    type Beat: Stream<Item = Result<(), Self::Error>>;
    type Layer;
    type Codec;
    fn heartbeat(&self) -> Self::Beat;
    fn middleware(&self) -> Self::Layer;
    fn poll(self, worker: &WorkerContext) -> Self::Stream;
}

pub trait Encoder<T> {
    type Error;
    type Compact;

    fn encode(val: &T) -> Result<Self::Compact, Self::Error>;
}

impl<T> Encoder<T> for () {
    type Error = io::Error;
    type Compact = ();
    fn encode(_val: &T) -> Result<Self::Compact, Self::Error> {
        Ok(())
    }
}
pub trait Decoder<T> {
    type Error;
    type Compact;

    fn decode(val: &Self::Compact) -> Result<T, Self::Error>;
}

impl<T: Default> Decoder<T> for () {
    type Error = io::Error;
    type Compact = ();
    fn decode(_: &Self::Compact) -> Result<T, Self::Error> {
        Ok(T::default())
    }
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
