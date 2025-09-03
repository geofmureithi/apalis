//! # Task Builder
//!
//! The `TaskBuilder` module provides a flexible builder pattern for constructing [`Task`] instances
//! with customizable configuration options. It allows users to specify arguments, context, extensions,
//! task identifiers, attempt information, status, and scheduling details for tasks.
//!
//! ## Features
//! - Create tasks with required arguments and optional context.
//! - Attach custom extensions/data to tasks.
//! - Assign unique task identifiers.
//! - Configure attempt and status information.
//! - Schedule tasks to run at specific times, after delays, or at intervals (seconds, minutes, hours).
//! - Build tasks with sensible defaults for omitted fields.
//!
//! ## Usage
//! Use [`TaskBuilder`] to incrementally configure a task, then call `.build()` to obtain a [`Task`] instance.
//! Convenience methods are provided for common scheduling scenarios.
//!
//! ### Example
//! ```rust
//! let task = TaskBuilder::new(args)
//!     .with_status(Status::Pending)
//!     .run_in_minutes(10)
//!     .build();
//! ```
//!
use crate::task::{
    attempt::Attempt, extensions::Extensions, metadata::MetadataExt, status::Status,
    task_id::TaskId, ExecutionContext, Task,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Builder for creating [`Task`] instances with optional configuration
#[derive(Debug)]
pub struct TaskBuilder<Args, Ctx, IdType> {
    args: Args,
    ctx: Ctx,
    data: Extensions,
    task_id: Option<TaskId<IdType>>,
    attempt: Option<Attempt>,
    status: Option<Status>,
    run_at: Option<u64>,
}

impl<Args, Ctx, IdType> TaskBuilder<Args, Ctx, IdType> {
    /// Create a new TaskBuilder with the required args
    pub fn new(args: Args) -> Self
    where
        Ctx: Default,
    {
        Self {
            args,
            ctx: Default::default(),
            data: Extensions::default(),
            task_id: None,
            attempt: None,
            status: None,
            run_at: None,
        }
    }

    /// Set the task's backend context
    pub fn with_ctx(mut self, ctx: Ctx) -> Self {
        self.ctx = ctx;
        self
    }

    /// Set the task's runtime data
    pub fn with_data(mut self, data: Extensions) -> Self {
        self.data = data;
        self
    }

    /// Insert a value into the task's data context
    pub fn data<D: Clone + Send + Sync + 'static>(mut self, value: D) -> Self {
        self.data.insert(value);
        self
    }

    /// Insert a value into the task's ctx context
    pub fn meta<M>(mut self, value: M) -> Self
    where
        Ctx: MetadataExt<M>,
        Ctx::Error: std::fmt::Debug,
    {
        self.ctx.inject(value).expect("Failed to inject ctx");
        self
    }

    /// Set the task ID
    pub fn with_task_id(mut self, task_id: TaskId<IdType>) -> Self {
        self.task_id = Some(task_id);
        self
    }

    /// Set the attempt information
    pub fn with_attempt(mut self, attempt: Attempt) -> Self {
        self.attempt = Some(attempt);
        self
    }

    /// Set the task status
    pub fn with_status(mut self, status: Status) -> Self {
        self.status = Some(status);
        self
    }

    /// Schedule the task to run at a specific Unix timestamp
    pub fn run_at_timestamp(mut self, timestamp: u64) -> Self {
        self.run_at = Some(timestamp);
        self
    }

    /// Schedule the task to run at a specific SystemTime
    pub fn run_at_time(mut self, time: SystemTime) -> Self {
        let timestamp = time
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        self.run_at = Some(timestamp);
        self
    }

    /// Schedule the task to run after a delay from now
    pub fn run_after(mut self, delay: Duration) -> Self {
        let now = SystemTime::now();
        let run_time = now + delay;
        let timestamp = run_time
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();
        self.run_at = Some(timestamp);
        self
    }

    /// Schedule the task to run in the specified number of seconds
    pub fn run_in_seconds(self, seconds: u64) -> Self {
        self.run_after(Duration::from_secs(seconds))
    }

    /// Schedule the task to run in the specified number of minutes
    pub fn run_in_minutes(self, minutes: u64) -> Self {
        self.run_after(Duration::from_secs(minutes * 60))
    }

    /// Schedule the task to run in the specified number of hours
    pub fn run_in_hours(self, hours: u64) -> Self {
        self.run_after(Duration::from_secs(hours * 3600))
    }

    /// Build the Task with default context
    pub fn build(self) -> Task<Args, Ctx, IdType> {
        let current_time = || {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs()
        };

        Task {
            args: self.args,
            ctx: ExecutionContext {
                task_id: self.task_id,
                data: self.data,
                attempt: self.attempt.unwrap_or_default(),
                backend_ctx: self.ctx,
                status: self.status.unwrap_or(Status::Pending),
                run_at: self.run_at.unwrap_or_else(current_time),
            },
        }
    }
}

// Convenience methods for Task to create a builder
impl<Args, Ctx: Default, IdType> Task<Args, Ctx, IdType> {
    /// Create a TaskBuilder with the given args
    pub fn builder(args: Args) -> TaskBuilder<Args, Ctx, IdType> {
        TaskBuilder::new(args)
    }
}
