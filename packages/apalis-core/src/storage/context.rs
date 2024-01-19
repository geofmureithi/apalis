use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::worker::WorkerId;

use super::job::{JobId, State};

/// The context for a job is represented here
/// Used to provide a context when a job is defined through the [Job] trait
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Context {
    pub(crate) id: JobId,
    pub(crate) status: State,
    pub(crate) run_at: i64,
    pub(crate) attempts: i32,
    pub(crate) max_attempts: i32,
    pub(crate) last_error: Option<String>,
    pub(crate) lock_at: Option<i64>,
    pub(crate) lock_by: Option<WorkerId>,
    pub(crate) done_at: Option<i64>,
}

impl Context {
    /// Build a new context with defaults given an ID.
    pub fn new(id: JobId) -> Self {
        let now: i64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .try_into()
            .unwrap();
        Context {
            id,
            status: State::Pending,
            run_at: now,
            lock_at: None,
            done_at: None,
            attempts: 0,
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
    pub fn id(&self) -> &JobId {
        &self.id
    }

    /// Gets the current attempts for a job. Default 0
    pub fn attempts(&self) -> i32 {
        self.attempts
    }

    /// Set the number of attempts
    pub fn set_attempts(&mut self, attempts: i32) {
        self.attempts = attempts;
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
    pub fn run_at(&self) -> i64 {
        self.run_at
    }

    /// Set the time a job should run
    pub fn set_run_at(&mut self, run_at: i64) {
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

    pub fn record_attempt(&mut self) {
        self.attempts += 1;
    }
}
