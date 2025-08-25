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
//! let req = Request::new_with_meta("send-email", "user-ctx");
//!
//! assert_eq!(req.args, "send-email");
//! assert_eq!(req.ctx.metadata, "user-ctx");
//! ```
//!
//! This module also defines helper types such as [`Attempt`], [`State`], [`TaskId`], and [`Extensions`] for managing task metadata.

use std::{
    fmt::Debug,
    time::{SystemTime, UNIX_EPOCH},
};


use crate::task::{attempt::Attempt, extensions::Extensions, status::Status, task_id::{TaskId, RandomId}};

pub mod attempt;
pub mod builder;
pub mod data;
pub mod extensions;
pub mod status;
pub mod task_id;
pub mod metadata;

/// Represents a task which will be executed
/// Should be considered a single unit of work
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Default)]
pub struct Task<Args, Meta, IdType = RandomId> {
    /// The inner request part
    pub args: Args,
    /// Context of the request eg id, attempts and context
    pub ctx: ExecutionContext<Meta, IdType>,
}

/// Component parts of a `Request`
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Default)]
pub struct ExecutionContext<Metadata, IdType = RandomId> {
    /// The task's id if allocated
    pub task_id: Option<TaskId<IdType>>,

    /// The tasks's extensions
    #[cfg_attr(feature = "serde", serde(skip))]
    pub data: Extensions,

    /// The tasks's attempts
    /// Keeps track of the number of attempts a task has been worked on
    pub attempt: Attempt,

    /// The task specific data provided by the backend
    pub metadata: Metadata,

    /// The task status
    pub status: Status,

    /// The time a task should be run
    pub run_at: u64,
}

impl<Meta, IdType: Clone> Clone for ExecutionContext<Meta, IdType>
where
    Meta: Clone,
{
    fn clone(&self) -> Self {
        Self {
            task_id: self.task_id.clone(),
            data: self.data.clone(),
            attempt: self.attempt.clone(),
            metadata: self.metadata.clone(),
            status: self.status.clone(),
            run_at: self.run_at,
        }
    }
}

impl<Args, Meta, IdType> Task<Args, Meta, IdType> {
    /// Creates a new [Request]
    pub fn new(args: Args) -> Self
    where
        Meta: Default,
    {
        Self::new_with_data(args, Extensions::default(), Meta::default())
    }

    /// Creates a request with all parts provided
    pub fn new_with_ctx(args: Args, ctx: ExecutionContext<Meta, IdType>) -> Self {
        Self { args, ctx }
    }

    /// Creates a request with metadata provided
    pub fn new_with_meta(req: Args, meta: Meta) -> Self {
        Self {
            args: req,
            ctx: ExecutionContext {
                metadata: meta,
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
    pub fn new_with_data(req: Args, data: Extensions, ctx: Meta) -> Self {
        Self {
            args: req,
            ctx: ExecutionContext {
                metadata: ctx,
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
    pub fn take(self) -> (Args, ExecutionContext<Meta, IdType>) {
        (self.args, self.ctx)
    }
}

impl<Args, Meta, IdType> Task<Args, Meta, IdType> {
    /// Maps the `args` field using the provided function, consuming the request.
    pub fn try_map<F, NewArgs, Err>(self, f: F) -> Result<Task<NewArgs, Meta, IdType>, Err>
    where
        F: FnOnce(Args) -> Result<NewArgs, Err>,
    {
        Ok(Task {
            args: f(self.args)?,
            ctx: self.ctx,
        })
    }
    /// Maps the `args` field using the provided function, consuming the request.
    pub fn map<F, NewArgs>(self, f: F) -> Task<NewArgs, Meta, IdType>
    where
        F: FnOnce(Args) -> NewArgs,
    {
        Task {
            args: f(self.args),
            ctx: self.ctx,
        }
    }

    /// Maps both `args` and `parts` together.
    pub fn map_all<F, NewArgs, NewCtx>(self, f: F) -> Task<NewArgs, NewCtx, IdType>
    where
        F: FnOnce(Args, ExecutionContext<Meta, IdType>) -> (NewArgs, ExecutionContext<NewCtx, IdType>),
    {
        let (args, parts) = f(self.args, self.ctx);
        Task { args, ctx: parts }
    }

    /// Maps only the `parts` field.
    pub fn map_parts<F, NewCtx>(self, f: F) -> Task<Args, NewCtx, IdType>
    where
        F: FnOnce(ExecutionContext<Meta, IdType>) -> ExecutionContext<NewCtx, IdType>,
    {
        Task {
            args: self.args,
            ctx: f(self.ctx),
        }
    }
}

impl<Args, Meta, IdType> std::ops::Deref for Task<Args, Meta, IdType> {
    type Target = Extensions;
    fn deref(&self) -> &Self::Target {
        &self.ctx.data
    }
}

impl<Args, Meta, IdType> std::ops::DerefMut for Task<Args, Meta, IdType> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ctx.data
    }
}
