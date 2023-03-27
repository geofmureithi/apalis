use crate::request::JobState;

use chrono::{DateTime, Utc};
use http::Extensions;
use serde::{Deserialize, Serialize};
use std::{any::Any, marker::Send};

/// The context for a job is represented here
/// Used to provide a context when a job is defined through the [Job] trait
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobContext {
    pub(crate) id: String,
    pub(crate) status: JobState,
    pub(crate) run_at: DateTime<Utc>,
    pub(crate) attempts: i32,
    pub(crate) max_attempts: i32,
    pub(crate) last_error: Option<String>,
    pub(crate) lock_at: Option<DateTime<Utc>>,
    pub(crate) lock_by: Option<String>,
    pub(crate) done_at: Option<DateTime<Utc>>,
    #[serde(skip)]
    pub(crate) data: Data,
}

#[derive(Debug, Default)]
pub(crate) struct Data(Extensions);

impl Clone for Data {
    fn clone(&self) -> Self {
        Data(Extensions::new())
    }
}

impl JobContext {
    /// Build a new context with defaults given an ID.
    #[must_use]
    pub fn new(id: String) -> Self {
        JobContext {
            id,
            status: JobState::Pending,
            run_at: Utc::now(),
            lock_at: None,
            done_at: None,
            attempts: 0,
            max_attempts: 25,
            last_error: None,
            lock_by: None,
            data: Data::default(),
        }
    }

    /// Get a reference to a type previously inserted on this `JobContext`.
    ///
    /// # Example
    ///
    /// ```
    /// # use apalis_core::context::JobContext;
    /// let mut ctx = JobContext::new(1.to_string());
    /// assert!(ctx.data_opt::<i32>().is_none());
    /// ctx.insert(5i32);
    ///
    /// assert_eq!(ctx.data_opt::<i32>(), Some(&5i32));
    /// ```
    #[must_use]
    pub fn data_opt<D: Any + Send + Sync>(&self) -> Option<&D> {
        self.data.0.get()
    }

    /// Insert a type into this `JobContext`.
    ///
    /// Important for embedding data for a job.
    /// If a extension of this type already existed, it will be returned.
    ///
    /// # Example
    ///
    /// ```
    /// # use apalis_core::context::JobContext;
    /// let mut ctx = JobContext::new(1.to_string());
    /// assert!(ctx.insert(5i32).is_none());
    /// assert!(ctx.insert(4u8).is_none());
    /// assert_eq!(ctx.insert(9i32), Some(5i32));
    /// ```
    pub fn insert<D: Any + Send + Sync>(&mut self, data: D) -> Option<D> {
        self.data.0.insert(data)
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
    pub fn id(&self) -> String {
        self.id.clone()
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
    pub fn done_at(&self) -> &Option<DateTime<Utc>> {
        &self.done_at
    }

    /// Set the time a job was done
    pub fn set_done_at(&mut self, done_at: Option<DateTime<Utc>>) {
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
    pub fn lock_at(&self) -> &Option<DateTime<Utc>> {
        &self.lock_at
    }

    /// Set the lock_at value
    pub fn set_lock_at(&mut self, lock_at: Option<DateTime<Utc>>) {
        self.lock_at = lock_at;
    }

    /// Get the job status
    pub fn status(&self) -> &JobState {
        &self.status
    }

    /// Set the job status
    pub fn set_status(&mut self, status: JobState) {
        self.status = status;
    }

    /// Get the time a job was locked
    pub fn lock_by(&self) -> &Option<String> {
        &self.lock_by
    }

    /// Set `lock_by`
    pub fn set_lock_by(&mut self, lock_by: Option<String>) {
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
}
