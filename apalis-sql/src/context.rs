use std::convert::Infallible;

use apalis_core::{
    task::{Task, metadata::MetadataExt},
    task_fn::FromRequest,
};

type JsonMapMetadata = serde_json::Map<String, serde_json::Value>;

use serde::{
    Deserialize, Serialize,
    de::{DeserializeOwned, Error},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlContext {
    max_attempts: i32,
    last_result: Option<serde_json::Value>,
    lock_at: Option<i64>,
    lock_by: Option<String>,
    done_at: Option<i64>,
    priority: i32,
    queue: Option<String>,
    meta: JsonMapMetadata,
}

impl Default for SqlContext {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlContext {
    /// Build a new context with defaults
    pub fn new() -> Self {
        SqlContext {
            lock_at: None,
            done_at: None,
            max_attempts: 5,
            last_result: None,
            lock_by: None,
            priority: 0,
            queue: None,
            meta: Default::default(),
        }
    }

    /// Set the number of attempts
    pub fn with_max_attempts(mut self, max_attempts: i32) -> Self {
        self.max_attempts = max_attempts;
        self
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
    pub fn with_done_at(mut self, done_at: Option<i64>) -> Self {
        self.done_at = done_at;
        self
    }

    /// Get the time a job was locked
    pub fn lock_at(&self) -> &Option<i64> {
        &self.lock_at
    }

    /// Set the lock_at value
    pub fn with_lock_at(mut self, lock_at: Option<i64>) -> Self {
        self.lock_at = lock_at;
        self
    }

    /// Get the time a job was locked
    pub fn lock_by(&self) -> &Option<String> {
        &self.lock_by
    }

    /// Set `lock_by`
    pub fn with_lock_by(mut self, lock_by: Option<String>) -> Self {
        self.lock_by = lock_by;
        self
    }

    /// Get the time a job was locked
    pub fn last_result(&self) -> &Option<serde_json::Value> {
        &self.last_result
    }

    /// Set the last result
    pub fn with_last_result(mut self, result: Option<serde_json::Value>) -> Self {
        self.last_result = result;
        self
    }

    /// Set the job priority. Larger values will run sooner. Default is 0.
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Get the job priority
    pub fn priority(&self) -> i32 {
        self.priority
    }

    pub fn queue(&self) -> &Option<String> {
        &self.queue
    }

    pub fn with_queue(mut self, queue: String) -> Self {
        self.queue = Some(queue);
        self
    }

    pub fn meta(&self) -> &JsonMapMetadata {
        &self.meta
    }

    pub fn with_meta(mut self, meta: JsonMapMetadata) -> Self {
        self.meta = meta;
        self
    }
}

impl<Args: Sync, IdType: Sync> FromRequest<Task<Args, Self, IdType>> for SqlContext {
    type Error = Infallible;
    async fn from_request(req: &Task<Args, Self, IdType>) -> Result<Self, Self::Error> {
        Ok(req.parts.ctx.clone())
    }
}

impl<T: DeserializeOwned + Serialize> MetadataExt<T> for SqlContext {
    type Error = serde_json::Error;
    fn extract(&self) -> Result<T, Self::Error> {
        self.meta
            .get(std::any::type_name::<T>())
            .and_then(|v| T::deserialize(v).ok())
            .ok_or(serde_json::Error::custom("Failed to extract metadata"))
    }
    fn inject(&mut self, value: T) -> Result<(), Self::Error> {
        self.meta.insert(
            std::any::type_name::<T>().to_string(),
            serde_json::to_value(&value).unwrap(),
        );
        Ok(())
    }
}
