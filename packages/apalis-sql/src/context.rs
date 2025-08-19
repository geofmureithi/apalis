use std::convert::Infallible;

use apalis_core::{
    service_fn::from_request::FromRequest,
    task::{status::Status, Task},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The context for a job is represented here
/// Used to provide a context for a job with an sql backend
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlMetadata {
    max_attempts: i32,
    last_error: Option<String>,
    lock_at: Option<i64>,
    lock_by: Option<String>,
    done_at: Option<i64>,
    priority: i32,
    extensions: Map<String, Value>,
}

impl Default for SqlMetadata {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlMetadata {
    /// Build a new context with defaults
    pub fn new() -> Self {
        SqlMetadata {
            lock_at: None,
            done_at: None,
            max_attempts: 5,
            last_error: None,
            lock_by: None,
            priority: 0,
            extensions: Map::new(),
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

    /// Get the time a job was done
    pub fn done_at(&self) -> &Option<i64> {
        &self.done_at
    }

    /// Set the time a job was done
    pub fn set_done_at(&mut self, done_at: Option<i64>) {
        self.done_at = done_at;
    }

    /// Get the time a job was locked
    pub fn lock_at(&self) -> &Option<i64> {
        &self.lock_at
    }

    /// Set the lock_at value
    pub fn set_lock_at(&mut self, lock_at: Option<i64>) {
        self.lock_at = lock_at;
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
    pub fn set_last_error(&mut self, error: Option<String>) {
        self.last_error = error;
    }

    /// Set the job priority. Larger values will run sooner. Default is 0.
    pub fn set_priority(&mut self, priority: i32) {
        self.priority = priority
    }

    /// Get the job priority
    pub fn priority(&self) -> &i32 {
        &self.priority
    }

    /// Get the job specific extensions
    pub fn extensions(&self) -> &Map<String, Value> {
        &self.extensions
    }

    /// Add extensions to the job meta
    pub fn insert<S: AsRef<str>, D: Serialize>(&mut self, key: S, data: D) {
        self.extensions
            .insert(key.as_ref().to_owned(), serde_json::to_value(data).unwrap());
    }
}

impl<Args: Sync, IdType: Send + Sync> FromRequest<Task<Args, SqlMetadata, IdType>> for SqlMetadata {
    type Error = Infallible;
    async fn from_request(req: &Task<Args, SqlMetadata, IdType>) -> Result<Self, Infallible> {
        Ok(req.ctx.metadata.clone())
    }
}
