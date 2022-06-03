use serde::{Deserialize, Serialize};

use std::fmt::Debug;
use strum::{AsRefStr, EnumString};

use crate::context::JobContext;

/// Represents the state of a [JobRequest] in a [Storage]
#[derive(EnumString, Serialize, Deserialize, Debug, Clone, AsRefStr)]
pub enum JobState {
    /// Job is pending
    #[serde(alias = "Latest")]
    Pending,
    /// Job is running
    Running,
    /// Job was done successfully
    Done,
    /// Retry Job
    Retry,
    /// Job has failed. Check `last_error`
    Failed,
    /// Job has been killed
    Killed,
}

impl Default for JobState {
    fn default() -> Self {
        JobState::Pending
    }
}

/// Represents a job which can be pushed and popped into a [Storage].

#[derive(Serialize, Debug, Deserialize, Clone)]
pub struct JobRequest<T> {
    pub(crate) job: T,
    pub(crate) context: JobContext,
}

impl<T> JobRequest<T> {
    /// Creates a new [JobRequest] ready to be pushed to a [Storage]
    pub fn new(job: T) -> Self {
        let id = uuid::Uuid::new_v4().to_string();
        let context = JobContext::new(id);
        Self { job, context }
    }

    /// Creates a Job request with context provided
    pub fn new_with_context(job: T, ctx: JobContext) -> Self {
        Self { job, context: ctx }
    }

    /// Get the underlying reference of the [Job]
    pub fn inner(&self) -> &T {
        &self.job
    }

    /// Gets a mutable reference to the job context.
    pub fn context_mut(&mut self) -> &mut JobContext {
        &mut self.context
    }

    /// Gets a reference to the job context.
    pub fn context(&self) -> &JobContext {
        &self.context
    }

    /// Records a job attempt
    pub fn record_attempt(&mut self) {
        self.context.set_attempts(self.context.attempts() + 1);
    }
}

impl<T> std::ops::Deref for JobRequest<T> {
    type Target = JobContext;
    fn deref(&self) -> &Self::Target {
        &self.context
    }
}

impl<T> std::ops::DerefMut for JobRequest<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.context
    }
}
