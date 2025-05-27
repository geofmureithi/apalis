use std::future::Future;

use futures::Stream;
use tower::{Layer, Service};

use crate::backend::Backend;

use super::{Event, WorkerContext, WorkerId};

pub type WorkerEvent<E> = Result<Option<(WorkerId, Event)>, E>;

pub trait WorkerStream<Req, Svc> {
    type Event;
    type Stream: Stream<Item = Event>;

    fn stream(self) -> Self::Stream;

    fn stream_with_ctx(self, ctx: &WorkerContext) -> Self::Stream;
}

pub trait MakeShared<Backend> {
    /// The Config for the backend
    type Config;
    /// The error returned if the backend cant be shared
    type MakeError;

    /// Returns the backend to be shared
    fn share(&mut self) -> Result<Backend, Self::MakeError>;

    /// Returns the backend with config
    fn share_with_config(&mut self, config: Self::Config) -> Result<Backend, Self::MakeError>;
}
