use futures::{future::BoxFuture, Stream};
use serde::{Deserialize, Serialize};
use tower::layer::util::Identity;

use std::{fmt::Debug, pin::Pin};

use crate::{
    data::Extensions,
    error::Error,
    poller::Poller,
    task::{attempt::Attempt, namespace::Namespace, task_id::TaskId},
    worker::WorkerId,
    Backend,
};

/// Represents a job which can be serialized and executed

#[derive(Serialize, Debug, Deserialize, Clone, Default)]
pub struct Request<Args, Ctx> {
    /// The inner request part
    pub args: Args,
    /// Parts of the request eg id, attempts and context
    pub parts: Parts<Ctx>,
}

/// Component parts of a `Request`
#[non_exhaustive]
#[derive(Serialize, Debug, Deserialize, Clone, Default)]
pub struct Parts<Ctx> {
    /// The request's id
    pub task_id: TaskId,

    /// The request's extensions
    #[serde(skip)]
    pub data: Extensions,

    /// The request's attempts
    pub attempt: Attempt,

    /// The Context stored by the storage
    pub context: Ctx,

    /// Represents the namespace
    #[serde(skip)]
    pub namespace: Option<Namespace>,
}

impl<T, Ctx: Default> Request<T, Ctx> {
    /// Creates a new [Request]
    pub fn new(args: T) -> Self {
        Self::new_with_data(args, Extensions::default(), Ctx::default())
    }

    /// Creates a request with all parts provided
    pub fn new_with_parts(args: T, parts: Parts<Ctx>) -> Self {
        Self { args, parts }
    }

    /// Creates a request with context provided
    pub fn new_with_ctx(req: T, ctx: Ctx) -> Self {
        Self {
            args: req,
            parts: Parts {
                context: ctx,
                ..Default::default()
            },
        }
    }

    /// Creates a request with data and context provided
    pub fn new_with_data(req: T, data: Extensions, ctx: Ctx) -> Self {
        Self {
            args: req,
            parts: Parts {
                context: ctx,
                data,
                ..Default::default()
            },
        }
    }

    /// Take the parts
    pub fn take_parts(self) -> (T, Parts<Ctx>) {
        (self.args, self.parts)
    }
}

impl<T, Ctx> std::ops::Deref for Request<T, Ctx> {
    type Target = Extensions;
    fn deref(&self) -> &Self::Target {
        &self.parts.data
    }
}

impl<T, Ctx> std::ops::DerefMut for Request<T, Ctx> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.parts.data
    }
}

/// Represents a stream that is send
pub type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;

/// Represents a result for a future that yields T
pub type RequestFuture<T> = BoxFuture<'static, T>;
/// Represents a stream for T.
pub type RequestStream<T> = BoxStream<'static, Result<Option<T>, Error>>;

impl<T, Res, Ctx> Backend<Request<T, Ctx>, Res> for RequestStream<Request<T, Ctx>> {
    type Stream = Self;

    type Layer = Identity;

    fn poll<Svc>(self, _worker: WorkerId) -> Poller<Self::Stream> {
        Poller {
            stream: self,
            heartbeat: Box::pin(async {}),
            layer: Identity::new(),
        }
    }
}
