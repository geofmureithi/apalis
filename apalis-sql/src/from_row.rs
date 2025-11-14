use std::str::FromStr;

use apalis_core::{
    backend::codec::Codec,
    error::BoxDynError,
    task::{Task, attempt::Attempt, builder::TaskBuilder, status::Status, task_id::TaskId},
};
use chrono::{DateTime, Utc};

use crate::context::SqlContext;

/// Errors that can occur when converting a database row into a Task
#[derive(Debug, thiserror::Error)]
pub enum FromRowError {
    /// Column not found in the row
    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    /// Error decoding the job data
    #[error("Decode error: {0}")]
    DecodeError(#[from] BoxDynError),
}

#[derive(Debug)]
/// Represents a row from the tasks table in the database.
///
/// This struct contains all the fields necessary to represent a task/job
/// stored in the SQL database, including its execution state, metadata,
/// and scheduling information.
pub struct TaskRow {
    /// The serialized job data as bytes
    pub job: Vec<u8>,
    /// Unique identifier for the task
    pub id: String,
    /// The type/name of the job being executed
    pub job_type: String,
    /// Current status of the task (e.g., "pending", "running", "completed", "failed")
    pub status: String,
    /// Number of times this task has been attempted
    pub attempts: usize,
    /// Maximum number of attempts allowed for this task before giving up
    pub max_attempts: Option<usize>,
    /// When the task should be executed (for scheduled tasks)
    pub run_at: Option<DateTime<Utc>>,
    /// The result of the last execution attempt, stored as JSON
    pub last_result: Option<serde_json::Value>,
    /// Timestamp when the task was locked for execution
    pub lock_at: Option<DateTime<Utc>>,
    /// Identifier of the worker/process that has locked this task
    pub lock_by: Option<String>,
    /// Timestamp when the task was completed
    pub done_at: Option<DateTime<Utc>>,
    /// Priority level of the task (higher values indicate higher priority)
    pub priority: Option<usize>,
    /// Additional metadata associated with the task, stored as JSON
    pub metadata: Option<serde_json::Value>,
}

impl TaskRow {
    /// Convert the TaskRow into a Task with decoded arguments
    pub fn try_into_task<D, Args, IdType>(
        self,
    ) -> Result<Task<Args, SqlContext, IdType>, FromRowError>
    where
        D::Error: Into<BoxDynError> + Send + Sync + 'static,
        IdType: FromStr,
        <IdType as FromStr>::Err: std::error::Error + Send + Sync + 'static,
        D: Codec<Args, Compact = Vec<u8>>,
        Args: 'static,
    {
        let ctx = SqlContext::default()
            .with_done_at(self.done_at.map(|dt| dt.timestamp()))
            .with_lock_by(self.lock_by)
            .with_max_attempts(self.max_attempts.unwrap_or(25) as i32)
            .with_last_result(self.last_result)
            .with_priority(self.priority.unwrap_or(0) as i32)
            .with_meta(
                self.metadata
                    .map(|m| {
                        serde_json::to_value(&m)
                            .unwrap_or_default()
                            .as_object()
                            .cloned()
                            .unwrap_or_default()
                    })
                    .unwrap_or_default(),
            )
            .with_queue(self.job_type)
            .with_lock_at(self.lock_at.map(|dt| dt.timestamp()));

        let args = D::decode(&self.job).map_err(|e| FromRowError::DecodeError(e.into()))?;
        let task = TaskBuilder::new(args)
            .with_ctx(ctx)
            .with_attempt(Attempt::new_with_value(self.attempts))
            .with_status(
                Status::from_str(&self.status).map_err(|e| FromRowError::DecodeError(e.into()))?,
            )
            .with_task_id(
                TaskId::from_str(&self.id).map_err(|e| FromRowError::DecodeError(e.into()))?,
            )
            .run_at_timestamp(
                self.run_at
                    .ok_or(FromRowError::ColumnNotFound("run_at".to_owned()))?
                    .timestamp() as u64,
            );
        Ok(task.build())
    }

    /// Convert the TaskRow into a Task with compacted arguments
    pub fn try_into_task_compact<IdType>(
        self,
    ) -> Result<Task<Vec<u8>, SqlContext, IdType>, FromRowError>
    where
        IdType: FromStr,
        <IdType as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    {
        let ctx = SqlContext::default()
            .with_done_at(self.done_at.map(|dt| dt.timestamp()))
            .with_lock_by(self.lock_by)
            .with_max_attempts(self.max_attempts.unwrap_or(25) as i32)
            .with_last_result(self.last_result)
            .with_priority(self.priority.unwrap_or(0) as i32)
            .with_meta(
                self.metadata
                    .map(|m| m.as_object().cloned().unwrap())
                    .unwrap_or_default(),
            )
            .with_queue(self.job_type)
            .with_lock_at(self.lock_at.map(|dt| dt.timestamp()));

        let task = TaskBuilder::new(self.job)
            .with_ctx(ctx)
            .with_attempt(Attempt::new_with_value(self.attempts))
            .with_status(
                Status::from_str(&self.status).map_err(|e| FromRowError::DecodeError(e.into()))?,
            )
            .with_task_id(
                TaskId::from_str(&self.id).map_err(|e| FromRowError::DecodeError(e.into()))?,
            )
            .run_at_timestamp(
                self.run_at
                    .ok_or(FromRowError::ColumnNotFound("run_at".to_owned()))?
                    .timestamp() as u64,
            );
        Ok(task.build())
    }
}
