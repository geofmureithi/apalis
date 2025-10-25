use std::str::FromStr;

use apalis_core::{
    backend::codec::Codec,
    error::BoxDynError,
    task::{Task, attempt::Attempt, builder::TaskBuilder, status::Status, task_id::TaskId},
};
use chrono::{DateTime, Utc};

use crate::context::SqlContext;

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
pub struct TaskRow {
    pub job: Vec<u8>,
    pub id: String,
    pub job_type: String,
    pub status: String,
    pub attempts: usize,
    pub max_attempts: Option<usize>,
    pub run_at: Option<DateTime<Utc>>,
    pub last_result: Option<serde_json::Value>,
    pub lock_at: Option<DateTime<Utc>>,
    pub lock_by: Option<String>,
    pub done_at: Option<DateTime<Utc>>,
    pub priority: Option<usize>,
    pub metadata: Option<serde_json::Value>,
}

impl TaskRow {
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

        // Optimize for the case where Args and CompactType are the same type
        // to avoid unnecessary serialization/deserialization.
        // That comes at the cost of using unsafe code, and leaking memory
        let args = if std::any::TypeId::of::<Args>() == std::any::TypeId::of::<Vec<u8>>()
        {
            // SAFETY: We've verified that Args and CompactType are the same type.
            // We use ptr::read to move the value out without calling drop on self.job.
            // Then we use mem::forget to prevent self from being dropped (which would
            // try to drop self.job again, causing a double free).
            unsafe {
                let job_ptr = &self.job as *const Vec<u8> as *const Args;
                let args = std::ptr::read(job_ptr);
                std::mem::forget(self.job);
                args
            }
        } else {
            D::decode(&self.job).map_err(|e| FromRowError::DecodeError(e.into()))?
        };
        let task = TaskBuilder::new(args)
            .with_ctx(ctx)
            .with_attempt(Attempt::new_with_value(self.attempts as usize))
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
            .with_status(Status::from_str(&self.status).map_err(|e| FromRowError::DecodeError(e.into()))?)
            .with_task_id(TaskId::from_str(&self.id).map_err(|e| FromRowError::DecodeError(e.into()))?)
            .run_at_timestamp(
                self.run_at
                    .ok_or(FromRowError::ColumnNotFound("run_at".to_owned()))?
                    .timestamp() as u64,
            );
        Ok(task.build())
    }
}
