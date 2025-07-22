//! Core request type used to represent tasks.
//!
//! A [`Request<Args, Ctx>`] encapsulates task input (`args`), contextual metadata (`Ctx`), and execution state.
//! It is passed to services that process tasks and provides access to task metadata like task ID, attempt count, and user-defined extensions.
//!
//! ## Structure
//!
//! - [`Request`] wraps the task arguments and metadata (`Parts`).
//! - [`Parts`] includes task ID, retry attempts, state, context, and extensible data.
//!
//! ## Example
//!
//! ```rust
//! use apalis_core::request::{Request, Parts};
//!
//! let req = Request::new_with_ctx("send-email", "user-ctx");
//!
//! assert_eq!(req.args, "send-email");
//! assert_eq!(req.parts.context, "user-ctx");
//! ```
//!
//! This module also defines helper types such as [`Attempt`], [`State`], [`TaskId`], and [`Extensions`] for managing task metadata.

use serde::{Deserialize, Serialize};

use std::{fmt::Debug, time::SystemTime};

use crate::request::{attempt::Attempt, extensions::Extensions, state::State, task_id::TaskId};

pub mod attempt;
pub mod data;
pub mod extensions;
pub mod state;
pub mod task_id;

/// Represents a task which can be serialized and executed

#[derive(Serialize, Debug, Deserialize, Clone, Default)]
pub struct Request<Args, Ctx> {
    /// The inner request part
    pub args: Args,
    /// Parts of the request eg id, attempts and context
    pub parts: Parts<Ctx>,
}

/// Component parts of a `Request`
// #[non_exhaustive]
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

    /// The task status
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

impl<Args, Ctx> Request<Args, Ctx> {
    /// Maps the `args` field using the provided function, consuming the request.
    pub fn try_map<F, NewArgs, Err>(self, f: F) -> Result<Request<NewArgs, Ctx>, Err>
    where
        F: FnOnce(Args) -> Result<NewArgs, Err>,
    {
        Ok(Request {
            args: f(self.args)?,
            parts: self.parts,
        })
    }
    /// Maps the `args` field using the provided function, consuming the request.
    pub fn map<F, NewArgs>(self, f: F) -> Request<NewArgs, Ctx>
    where
        F: FnOnce(Args) -> NewArgs,
    {
        Request {
            args: f(self.args),
            parts: self.parts,
        }
    }

    /// Maps the `args` field by reference.
    pub fn map_ref<F, NewArgs>(&self, f: F) -> Request<NewArgs, Ctx>
    where
        F: FnOnce(&Args) -> NewArgs,
        Ctx: Clone, // Needed to clone parts if they contain references
    {
        Request {
            args: f(&self.args),
            parts: self.parts.clone(),
        }
    }

    /// Maps both `args` and `parts` together.
    pub fn map_all<F, NewArgs, NewCtx>(self, f: F) -> Request<NewArgs, NewCtx>
    where
        F: FnOnce(Args, Parts<Ctx>) -> (NewArgs, Parts<NewCtx>),
    {
        let (args, parts) = f(self.args, self.parts);
        Request { args, parts }
    }

    /// Maps only the `parts` field.
    pub fn map_parts<F, NewCtx>(self, f: F) -> Request<Args, NewCtx>
    where
        F: FnOnce(Parts<Ctx>) -> Parts<NewCtx>,
    {
        Request {
            args: self.args,
            parts: f(self.parts),
        }
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
