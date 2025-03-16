use futures::{future::BoxFuture, Stream};
use serde::{Deserialize, Serialize};
use tower::layer::util::Identity;

use std::{fmt, fmt::Debug, pin::Pin, str::FromStr};

use crate::{
    backend::Backend,
    codec::NoopCodec,
    data::Extensions,
    error::Error,
    poller::Poller,
    task::{attempt::Attempt, namespace::Namespace, task_id::TaskId},
    worker::{Context, Worker},
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
    //TODO: add State
}

impl<T, Ctx> Request<T, Ctx> {
    /// Creates a new [Request]
    pub fn new(args: T) -> Self
    where
        Ctx: Default,
    {
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
                task_id: Default::default(),
                attempt: Default::default(),
                data: Default::default(),
                namespace: Default::default(),
            },
        }
    }

    /// Creates a request with data and context provided
    pub fn new_with_data(req: T, data: Extensions, ctx: Ctx) -> Self {
        Self {
            args: req,
            parts: Parts {
                context: ctx,
                task_id: Default::default(),
                attempt: Default::default(),
                data,
                namespace: Default::default(),
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

/// Represents the state of a job/task
#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, std::cmp::Eq)]
pub enum State {
    /// Job is pending
    #[serde(alias = "Latest")]
    Pending,
    /// Job is in the queue but not ready for execution
    Scheduled,
    /// Job is running
    Running,
    /// Job was done successfully
    Done,
    /// Job has failed. Check `last_error`
    Failed,
    /// Job has been killed
    Killed,
}

impl Default for State {
    fn default() -> Self {
        State::Pending
    }
}

impl FromStr for State {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Pending" | "Latest" => Ok(State::Pending),
            "Running" => Ok(State::Running),
            "Done" => Ok(State::Done),
            "Failed" => Ok(State::Failed),
            "Killed" => Ok(State::Killed),
            "Scheduled" => Ok(State::Scheduled),
            _ => Err(Error::MissingData("Invalid Job state".to_string())),
        }
    }
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            State::Pending => write!(f, "Pending"),
            State::Running => write!(f, "Running"),
            State::Done => write!(f, "Done"),
            State::Failed => write!(f, "Failed"),
            State::Killed => write!(f, "Killed"),
            State::Scheduled => write!(f, "Scheduled"),
        }
    }
}

/// Represents a stream that is send
pub type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;

/// Represents a result for a future that yields T
pub type RequestFuture<T> = BoxFuture<'static, T>;
/// Represents a stream for T.
pub type RequestStream<T> = BoxStream<'static, Result<Option<T>, Error>>;

impl<T, Ctx> Backend<Request<T, Ctx>> for RequestStream<Request<T, Ctx>> {
    type Stream = Self;

    type Layer = Identity;

    type Codec = NoopCodec<Request<T, Ctx>>;

    fn poll(self, _worker: &Worker<Context>) -> Poller<Self::Stream> {
        Poller {
            stream: self,
            heartbeat: Box::pin(futures::future::pending()),
            layer: Identity::new(),
            _priv: (),
        }
    }
}
