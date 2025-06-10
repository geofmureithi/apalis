use futures::{future::BoxFuture, stream, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tower::layer::util::Identity;

use std::{
    fmt::{self, Debug},
    future::ready,
    pin::Pin,
    str::FromStr,
};

use crate::{
    backend::Backend,
    data::Extensions,
    error::BoxDynError,
    task::{attempt::Attempt, task_id::TaskId},
    worker::WorkerContext,
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

    pub state: State,
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
                state: State::Pending,
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
                state: State::Pending,
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

/// Represents the state of a task
#[non_exhaustive]
#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, std::cmp::Eq)]
pub enum State {
    /// Job is pending
    #[serde(alias = "Latest")]
    Pending,
    /// Job is queued for execution, but no worker has picked it up
    Queued,
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

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("Unknown state: {0}")]
    UnknownState(String),
}

impl FromStr for State {
    type Err = StateError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Pending" => Ok(State::Pending),
            "Queued" => Ok(State::Queued),
            "Running" => Ok(State::Running),
            "Done" => Ok(State::Done),
            "Failed" => Ok(State::Failed),
            "Killed" => Ok(State::Killed),
            _ => Err(StateError::UnknownState(s.to_owned())),
        }
    }
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            State::Pending => write!(f, "Pending"),
            State::Queued => write!(f, "Queued"),
            State::Running => write!(f, "Running"),
            State::Done => write!(f, "Done"),
            State::Failed => write!(f, "Failed"),
            State::Killed => write!(f, "Killed"),
        }
    }
}

/// Represents a stream that is send
pub type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;

/// Represents a result for a future that yields T
pub type RequestFuture<T> = BoxFuture<'static, T>;
/// Represents a stream for T.
pub type RequestStream<T> = BoxStream<'static, Result<Option<T>, BoxDynError>>;

impl<T, Ctx> Backend<Request<T, Ctx>> for RequestStream<Request<T, Ctx>> {
    type Error = BoxDynError;
    type Stream = Self;
    type Layer = Identity;
    type Beat = BoxStream<'static, Result<(), BoxDynError>>;
    fn heartbeat(&self) -> Self::Beat {
        stream::once(ready(Ok(()))).boxed()
    }
    fn middleware(&self) -> Self::Layer {
        Identity::new()
    }
    fn poll(self, _: &WorkerContext) -> Self::Stream {
        self
    }
}
