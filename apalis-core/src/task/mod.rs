//! Utilities for creating and managing tasks.
//!
//! The [`Task`] component encapsulates a unit of work to be executed,
//! along with its associated context, metadata, and execution status. The [`Parts`]
//! struct contains metadata, attempt tracking, extensions, and scheduling information for each task.
//!
//! # Overview
//!
//! In `apalis`, tasks are designed to represent discrete units of work that can be scheduled, retried, and tracked
//! throughout their lifecycle. Each task consists of arguments (`args`) describing the work to be performed,
//! and an [`Parts`] (`parts`) containing metadata and control information.
//!
//! ## [`Task`]
//!
//! The [`Task`] struct is generic over:
//! - `Args`: The type of arguments or payload for the task.
//! - `Ctx`: Ctxdata associated with the task, such as custom fields or backend-specific information.
//! - `IdType`: The type used for uniquely identifying the task (defaults to [`RandomId`]).
//!
//! ## [`Parts`]
//!
//! The [`Parts`] struct provides the following:
//! - `task_id`: Optionally stores a unique identifier for the task.
//! - `data`: An [`Extensions`] container for storing arbitrary per-task data (e.g., middleware extensions).
//! - `attempt`: Tracks how many times the task has been attempted.
//! - `metadata`: Custom metadata for the task, provided by the backend or user.
//! - `status`: The current [`Status`] of the task (e.g., Pending, Running, Completed, Failed).
//! - `run_at`: The UNIX timestamp (in seconds) when the task should be run.
//!
//! The execution context is essential for tracking the state and metadata of a task as it moves through
//! the system. It enables features such as retries, scheduling, and extensibility via the `Extensions` type.
//!
//! # Modules
//!
//! - [`attempt`]: Tracks the number of attempts a task has been executed.
//! - [`builder`]: Utilities for constructing tasks.
//! - [`data`]: Data types for task payloads.
//! - [`extensions`]: Extension storage for tasks.
//! - [`metadata`]: Ctxdata types for tasks.
//! - [`status`]: Status tracking for tasks.
//! - [`task_id`]: Types for uniquely identifying tasks.
//!
//! # Examples
//!
//! ## Creating a new task with default metadata
//!
//! ```rust
//! # use apalis_core::task::{Task, Parts};
//! # use apalis_core::task::builder::TaskBuilder;
//! let task: Task<String, ()> = TaskBuilder::new("my work".to_string()).build();
//! ```
//!
//! ## Creating a task with custom metadata
//!
//! ```rust
//! # use apalis_core::task::{Task, Parts};
//! # use apalis_core::task::builder::TaskBuilder;
//! # use apalis_core::task::extensions::Extensions;
//!
//! #[derive(Default, Clone)]
//! struct MyCtx { priority: u8 }
//! let task: Task<String, Extensions> = TaskBuilder::new("important work".to_string())
//!     .meta(MyCtx { priority: 5 })
//!     .build();
//! ```
//!
//! ## Accessing and modifying the execution context
//!
//! ```rust
//! use apalis_core::task::{Task, Parts, status::Status};
//! let mut task = Task::<String, ()>::new("work".to_string());
//! task.parts.status = Status::Running.into();
//! task.parts.attempt.increment();
//! ```
//!
//! ## Using Extensions for per-task data
//!
//! ```rust
//! # use apalis_core::task::builder::TaskBuilder;
//! use apalis_core::task::{Task, extensions::Extensions};
//! #[derive(Debug, Clone, PartialEq)]
//! pub struct TracingId(String);
//! let mut extensions = Extensions::default();
//! extensions.insert(TracingId("abc123".to_owned()));
//! let task: Task<String, ()> = TaskBuilder::new("work".to_string()).with_data(extensions).build();
//! assert_eq!(task.parts.data.get::<TracingId>(), Some(&TracingId("abc123".to_owned())));
//! ```
//!
//! # See Also
//!
//! - [`Task`]: Represents a unit of work to be executed.
//! - [`Parts`]: Holds metadata, status, and control information for a task.
//! - [`Extensions`]: Type-safe storage for per-task data.
//! - [`Status`]: Enum representing the lifecycle state of a task.
//! - [`Attempt`]: Tracks the number of execution attempts for a task.
//! - [`TaskId`]: Unique identifier type for tasks.
//! - [`FromRequest`]: Trait for extracting data from task contexts.
//! - [`IntoResponse`]: Trait for converting tasks into response types.
//! - [`TaskBuilder`]: Fluent builder for constructing tasks with optional configuration.
//! - [`RandomId`]: Default unique identifier type for tasks.
//!
//! [`TaskBuilder`]: crate::task::builder::TaskBuilder
//! [`IntoResponse`]: crate::task_fn::into_response::IntoResponse
//! [`FromRequest`]: crate::task_fn::from_request::FromRequest

use std::{
    fmt::Debug,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    task::{
        attempt::Attempt,
        builder::TaskBuilder,
        extensions::Extensions,
        status::{AtomicStatus, Status},
        task_id::TaskId,
    },
    task_fn::FromRequest,
};

pub mod attempt;
pub mod builder;
pub mod data;
pub mod extensions;
pub mod metadata;
pub mod status;
pub mod task_id;

/// Represents a task which will be executed
/// Should be considered a single unit of work
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Default)]
pub struct Task<Args, Context, IdType> {
    /// The argument task part
    pub args: Args,
    /// Parts of the task eg id, attempts and context
    pub parts: Parts<Context, IdType>,
}

/// Component parts of a `Task`
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Default)]
pub struct Parts<Context, IdType> {
    /// The task's id if allocated
    pub task_id: Option<TaskId<IdType>>,

    /// The tasks's extensions
    #[cfg_attr(feature = "serde", serde(skip))]
    pub data: Extensions,

    /// The tasks's attempts
    /// Keeps track of the number of attempts a task has been worked on
    pub attempt: Attempt,

    /// The task specific data provided by the backend
    pub ctx: Context,

    /// The task status that is wrapped in an atomic status
    pub status: AtomicStatus,

    /// The time a task should be run
    pub run_at: u64,
}

impl<Ctx: Debug, IdType: Debug> Debug for Parts<Ctx, IdType> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Parts")
            .field("task_id", &self.task_id)
            .field("data", &"<Extensions>")
            .field("attempt", &self.attempt)
            .field("ctx", &self.ctx)
            .field("status", &self.status.load())
            .field("run_at", &self.run_at)
            .finish()
    }
}

impl<Ctx, IdType: Clone> Clone for Parts<Ctx, IdType>
where
    Ctx: Clone,
{
    fn clone(&self) -> Self {
        Self {
            task_id: self.task_id.clone(),
            data: self.data.clone(),
            attempt: self.attempt.clone(),
            ctx: self.ctx.clone(),
            status: self.status.clone(),
            run_at: self.run_at,
        }
    }
}

impl<Args, Ctx, IdType> Task<Args, Ctx, IdType> {
    /// Creates a new [Task]
    pub fn new(args: Args) -> Self
    where
        Ctx: Default,
    {
        Self::new_with_data(args, Extensions::default(), Ctx::default())
    }

    /// Creates a task with context provided
    pub fn new_with_ctx(req: Args, ctx: Ctx) -> Self {
        Self {
            args: req,
            parts: Parts {
                ctx,
                task_id: Default::default(),
                attempt: Default::default(),
                data: Default::default(),
                status: Status::Pending.into(),
                run_at: {
                    let now = SystemTime::now();
                    let duration_since_epoch =
                        now.duration_since(UNIX_EPOCH).expect("Time went backwards");
                    duration_since_epoch.as_secs()
                },
            },
        }
    }

    /// Creates a task with data and context provided
    pub fn new_with_data(req: Args, data: Extensions, ctx: Ctx) -> Self {
        Self {
            args: req,
            parts: Parts {
                ctx,
                task_id: Default::default(),
                attempt: Default::default(),
                data,
                status: Status::Pending.into(),
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
    pub fn take(self) -> (Args, Parts<Ctx, IdType>) {
        (self.args, self.parts)
    }

    /// Extract a value of type `T` from the task's context
    ///
    /// Uses [FromRequest] trait to extract the value.
    pub async fn extract<T: FromRequest<Self>>(&self) -> Result<T, T::Error> {
        T::from_request(self).await
    }

    /// Converts the task into a builder pattern.
    pub fn into_builder(self) -> TaskBuilder<Args, Ctx, IdType> {
        TaskBuilder {
            args: self.args,
            ctx: self.parts.ctx,
            attempt: Some(self.parts.attempt),
            data: self.parts.data,
            status: Some(self.parts.status.into()),
            run_at: Some(self.parts.run_at),
            task_id: self.parts.task_id,
        }
    }
}

impl<Args, Ctx, IdType> Task<Args, Ctx, IdType> {
    /// Maps the `args` field using the provided function, consuming the task.
    pub fn try_map<F, NewArgs, Err>(self, f: F) -> Result<Task<NewArgs, Ctx, IdType>, Err>
    where
        F: FnOnce(Args) -> Result<NewArgs, Err>,
    {
        Ok(Task {
            args: f(self.args)?,
            parts: self.parts,
        })
    }
    /// Maps the `args` field using the provided function, consuming the task.
    pub fn map<F, NewArgs>(self, f: F) -> Task<NewArgs, Ctx, IdType>
    where
        F: FnOnce(Args) -> NewArgs,
    {
        Task {
            args: f(self.args),
            parts: self.parts,
        }
    }

    /// Maps both `args` and `parts` together.
    pub fn map_all<F, NewArgs, NewCtx>(self, f: F) -> Task<NewArgs, NewCtx, IdType>
    where
        F: FnOnce(Args, Parts<Ctx, IdType>) -> (NewArgs, Parts<NewCtx, IdType>),
    {
        let (args, parts) = f(self.args, self.parts);
        Task { args, parts }
    }

    /// Maps only the `parts` field.
    pub fn map_parts<F, NewCtx>(self, f: F) -> Task<Args, NewCtx, IdType>
    where
        F: FnOnce(Parts<Ctx, IdType>) -> Parts<NewCtx, IdType>,
    {
        Task {
            args: self.args,
            parts: f(self.parts),
        }
    }
}
