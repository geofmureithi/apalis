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

use std::{
    fmt::Debug,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::task::{attempt::Attempt, extensions::Extensions, status::Status, task_id::TaskId};

pub mod attempt;
pub mod data;
pub mod extensions;
pub mod status;
pub mod task_id;
pub mod builder;

/// Represents a task which can be serialized and executed
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Default)]
pub struct Task<Args, Ctx> {
    /// The inner request part
    pub args: Args,
    /// Parts of the request eg id, attempts and context
    pub meta: Metadata<Ctx>,
}

/// Component parts of a `Request`
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Default)]
pub struct Metadata<Ctx> {
    /// The request's id
    pub task_id: TaskId,

    /// The request's extensions
    #[cfg_attr(feature = "serde", serde(skip))]
    pub data: Extensions,

    /// The request's attempts
    pub attempt: Attempt,

    /// The request specific data stored by the backend
    pub context: Ctx,

    /// The task status
    pub status: Status,

    /// The time a task should be run
    pub run_at: u64,
}

impl<T, Ctx> Task<T, Ctx> {
    /// Creates a new [Request]
    pub fn new(args: T) -> Self
    where
        Ctx: Default,
    {
        Self::new_with_data(args, Extensions::default(), Ctx::default())
    }

    /// Creates a request with all parts provided
    pub fn new_with_parts(args: T, parts: Metadata<Ctx>) -> Self {
        Self { args, meta: parts }
    }

    /// Creates a request with context provided
    pub fn new_with_ctx(req: T, ctx: Ctx) -> Self {
        Self {
            args: req,
            meta: Metadata {
                context: ctx,
                task_id: Default::default(),
                attempt: Default::default(),
                data: Default::default(),
                status: Status::Pending,
                run_at: {
                    let now = SystemTime::now();
                    let duration_since_epoch =
                        now.duration_since(UNIX_EPOCH).expect("Time went backwards");
                    duration_since_epoch.as_secs()
                },
            },
        }
    }

    /// Creates a request with data and context provided
    pub fn new_with_data(req: T, data: Extensions, ctx: Ctx) -> Self {
        Self {
            args: req,
            meta: Metadata {
                context: ctx,
                task_id: Default::default(),
                attempt: Default::default(),
                data,
                status: Status::Pending,
                run_at: {
                    let now = SystemTime::now();
                    let duration_since_epoch =
                        now.duration_since(UNIX_EPOCH).expect("Time went backwards");
                    duration_since_epoch.as_secs()
                },
            },
        }
    }

    /// Take the task into its parts
    pub fn take(self) -> (T, Metadata<Ctx>) {
        (self.args, self.meta)
    }
}

impl<Args, Ctx> Task<Args, Ctx> {
    /// Maps the `args` field using the provided function, consuming the request.
    pub fn try_map<F, NewArgs, Err>(self, f: F) -> Result<Task<NewArgs, Ctx>, Err>
    where
        F: FnOnce(Args) -> Result<NewArgs, Err>,
    {
        Ok(Task {
            args: f(self.args)?,
            meta: self.meta,
        })
    }
    /// Maps the `args` field using the provided function, consuming the request.
    pub fn map<F, NewArgs>(self, f: F) -> Task<NewArgs, Ctx>
    where
        F: FnOnce(Args) -> NewArgs,
    {
        Task {
            args: f(self.args),
            meta: self.meta,
        }
    }

    /// Maps the `args` field by reference.
    pub fn map_ref<F, NewArgs>(&self, f: F) -> Task<NewArgs, Ctx>
    where
        F: FnOnce(&Args) -> NewArgs,
        Ctx: Clone, // Needed to clone parts if they contain references
    {
        Task {
            args: f(&self.args),
            meta: self.meta.clone(),
        }
    }

    /// Maps both `args` and `parts` together.
    pub fn map_all<F, NewArgs, NewCtx>(self, f: F) -> Task<NewArgs, NewCtx>
    where
        F: FnOnce(Args, Metadata<Ctx>) -> (NewArgs, Metadata<NewCtx>),
    {
        let (args, parts) = f(self.args, self.meta);
        Task { args, meta: parts }
    }

    /// Maps only the `parts` field.
    pub fn map_parts<F, NewCtx>(self, f: F) -> Task<Args, NewCtx>
    where
        F: FnOnce(Metadata<Ctx>) -> Metadata<NewCtx>,
    {
        Task {
            args: self.args,
            meta: f(self.meta),
        }
    }
}

impl<T, Ctx> std::ops::Deref for Task<T, Ctx> {
    type Target = Extensions;
    fn deref(&self) -> &Self::Target {
        &self.meta.data
    }
}

impl<T, Ctx> std::ops::DerefMut for Task<T, Ctx> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.meta.data
    }
}
