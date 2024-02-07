use apalis_core::error::Error;
use apalis_core::task::{attempt::Attempt, task_id::TaskId};
use apalis_core::worker::WorkerId;
use serde::{Deserialize, Serialize};
use sqlx::types::chrono::{DateTime, Utc};
use std::{fmt, str::FromStr};

/// The context for a job is represented here
/// Used to provide a context when a job is defined through the [Job] trait
#[derive(Debug, Clone)]
pub struct SqlContext {
    id: TaskId,
    status: State,
    run_at: DateTime<Utc>,
    attempts: Attempt,
    max_attempts: i32,
    last_error: Option<String>,
    lock_at: Option<i64>,
    lock_by: Option<WorkerId>,
    done_at: Option<i64>,
}

impl SqlContext {
    /// Build a new context with defaults given an ID.
    pub fn new(id: TaskId) -> Self {
        SqlContext {
            id,
            status: State::Pending,
            run_at: Utc::now(),
            lock_at: None,
            done_at: None,
            attempts: Default::default(),
            max_attempts: 25,
            last_error: None,
            lock_by: None,
        }
    }

    /// Set the number of attempts
    pub fn set_max_attempts(&mut self, max_attempts: i32) {
        self.max_attempts = max_attempts;
    }

    /// Gets the maximum attempts for a job. Default 25
    pub fn max_attempts(&self) -> i32 {
        self.max_attempts
    }

    /// Get the id for a job
    pub fn id(&self) -> &TaskId {
        &self.id
    }

    /// Gets the current attempts for a job. Default 0
    pub fn attempts(&self) -> &Attempt {
        &self.attempts
    }

    /// Set the number of attempts
    pub fn set_attempts(&mut self, attempts: i32) {
        self.attempts = Attempt::new_with_value(attempts.try_into().unwrap());
    }

    /// Get the time a job was done
    pub fn done_at(&self) -> &Option<i64> {
        &self.done_at
    }

    /// Set the time a job was done
    pub fn set_done_at(&mut self, done_at: Option<i64>) {
        self.done_at = done_at;
    }

    /// Get the time a job is supposed to start
    pub fn run_at(&self) -> &DateTime<Utc> {
        &self.run_at
    }

    /// Set the time a job should run
    pub fn set_run_at(&mut self, run_at: DateTime<Utc>) {
        self.run_at = run_at;
    }

    /// Get the time a job was locked
    pub fn lock_at(&self) -> &Option<i64> {
        &self.lock_at
    }

    /// Set the lock_at value
    pub fn set_lock_at(&mut self, lock_at: Option<i64>) {
        self.lock_at = lock_at;
    }

    /// Get the job status
    pub fn status(&self) -> &State {
        &self.status
    }

    /// Set the job status
    pub fn set_status(&mut self, status: State) {
        self.status = status;
    }

    /// Get the time a job was locked
    pub fn lock_by(&self) -> &Option<WorkerId> {
        &self.lock_by
    }

    /// Set `lock_by`
    pub fn set_lock_by(&mut self, lock_by: Option<WorkerId>) {
        self.lock_by = lock_by;
    }

    /// Get the time a job was locked
    pub fn last_error(&self) -> &Option<String> {
        &self.last_error
    }

    /// Set the last error
    pub fn set_last_error(&mut self, error: String) {
        self.last_error = Some(error);
    }

    /// Record an attempt to execute the request
    pub fn record_attempt(&mut self) {
        self.attempts.increment();
    }
}

/// Represents the state of a [Request]
#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, std::cmp::Eq)]
pub enum State {
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
            "Retry" => Ok(State::Retry),
            "Failed" => Ok(State::Failed),
            "Killed" => Ok(State::Killed),
            _ => Err(Error::InvalidContext("Invalid Job state".to_string())),
        }
    }
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            State::Pending => write!(f, "Pending"),
            State::Running => write!(f, "Running"),
            State::Done => write!(f, "Done"),
            State::Retry => write!(f, "Retry"),
            State::Failed => write!(f, "Failed"),
            State::Killed => write!(f, "Killed"),
        }
    }
}
