use crate::task::{
    attempt::Attempt, extensions::Extensions, status::Status, task_id::TaskId, Metadata, Task,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Builder for creating `Task` instances with optional configuration
pub struct TaskBuilder<Args, Ctx> {
    args: Args,
    context: Option<Ctx>,
    data: Option<Extensions>,
    task_id: Option<TaskId>,
    attempt: Option<Attempt>,
    status: Option<Status>,
    run_at: Option<u64>,
}

impl<Args, Ctx> TaskBuilder<Args, Ctx> {
    /// Create a new TaskBuilder with the required args
    pub fn new(args: Args) -> Self {
        Self {
            args,
            context: None,
            data: None,
            task_id: None,
            attempt: None,
            status: None,
            run_at: None,
        }
    }

    /// Set the task context
    pub fn with_context(mut self, context: Ctx) -> Self {
        self.context = Some(context);
        self
    }

    /// Set the task extensions/data
    pub fn with_data(mut self, data: Extensions) -> Self {
        self.data = Some(data);
        self
    }

    /// Set the task ID
    pub fn with_task_id(mut self, task_id: TaskId) -> Self {
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
    pub fn run_in_seconds(mut self, seconds: u64) -> Self {
        self.run_after(Duration::from_secs(seconds))
    }

    /// Schedule the task to run in the specified number of minutes
    pub fn run_in_minutes(mut self, minutes: u64) -> Self {
        self.run_after(Duration::from_secs(minutes * 60))
    }

    /// Schedule the task to run in the specified number of hours
    pub fn run_in_hours(mut self, hours: u64) -> Self {
        self.run_after(Duration::from_secs(hours * 3600))
    }

    /// Build the Task with default context
    pub fn build(self) -> Task<Args, Ctx>
    where
        Ctx: Default,
    {
        let current_time = || {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs()
        };

        Task {
            args: self.args,
            meta: Metadata {
                task_id: self.task_id.unwrap_or_default(),
                data: self.data.unwrap_or_default(),
                attempt: self.attempt.unwrap_or_default(),
                context: self.context.unwrap_or_default(),
                status: self.status.unwrap_or(Status::Pending),
                run_at: self.run_at.unwrap_or_else(current_time),
            },
        }
    }

    /// Build the Task with provided default context
    pub fn build_with_default_context(self, default_context: Ctx) -> Task<Args, Ctx> {
        let current_time = || {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs()
        };

        Task {
            args: self.args,
            meta: Metadata {
                task_id: self.task_id.unwrap_or_default(),
                data: self.data.unwrap_or_default(),
                attempt: self.attempt.unwrap_or_default(),
                context: self.context.unwrap_or(default_context),
                status: self.status.unwrap_or(Status::Pending),
                run_at: self.run_at.unwrap_or_else(current_time),
            },
        }
    }
}

// Convenience methods for Task to create a builder
impl<Args, Ctx> Task<Args, Ctx> {
    /// Create a TaskBuilder with the given args
    pub fn builder(args: Args) -> TaskBuilder<Args, Ctx> {
        TaskBuilder::new(args)
    }
}
