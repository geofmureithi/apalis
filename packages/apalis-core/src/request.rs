use crate::service::JobService;
use crate::storage::Storage;
use chrono::{DateTime, TimeZone, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::{sqlite::SqliteRow, FromRow, Row};
use std::fmt::Debug;
use strum::EnumString;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::{
    context::JobContext,
    error::JobError,
    job::Job,
    response::{JobResponse, JobResult},
};

/// Represents the state of a [JobRequest] in a [Storage]
#[derive(EnumString, Serialize, Deserialize, Debug, Clone)]
pub enum JobState {
    Pending,
    Running,
    Done,
    Retry,
    Failed,
    Killed,
}

/// Represents a job which can be pushed and popped into a [Storage].
///
///
/// Its usually passed to a [JobService] for execution.
#[derive(Serialize, Debug, Deserialize)]
pub struct JobRequest<T> {
    job: T,
    id: String,
    status: JobState,
    run_at: DateTime<Utc>,
    attempts: i32,
    max_attempts: i32,
    last_error: Option<String>,
    lock_at: Option<DateTime<Utc>>,
    lock_by: Option<String>,
    done_at: Option<DateTime<Utc>>,

    #[serde(skip)]
    context: JobContext,
}

impl<T: Clone> Clone for JobRequest<T> {
    fn clone(&self) -> Self {
        Self {
            job: self.job.clone(),
            id: self.id.clone(),
            status: self.status.clone(),
            run_at: self.run_at.clone(),
            lock_at: self.lock_at.clone(),
            done_at: self.done_at.clone(),
            attempts: self.attempts,
            max_attempts: self.max_attempts,
            last_error: self.last_error.clone(),
            lock_by: self.lock_by.clone(),
            context: JobContext::new(),
        }
    }
}

impl<T> JobRequest<T> {
    /// Creates a new [JobRequest] ready to be pushed to a [Storage]
    pub fn new(job: T) -> Self {
        Self {
            job,
            id: uuid::Uuid::new_v4().to_string(),
            status: JobState::Pending,
            run_at: Utc::now(),
            lock_at: None,
            done_at: None,
            attempts: 0,
            max_attempts: 25,
            last_error: None,
            lock_by: None,
            context: JobContext::new(),
        }
    }

    /// Get the underlying reference of the [Job]
    pub fn inner(&self) -> &T {
        &self.job
    }

    /// Get the [Uuid] for a job
    pub fn id(&self) -> String {
        self.id.clone()
    }

    /// Gets a mutable reference to the job context.
    pub fn context_mut(&mut self) -> &mut JobContext {
        &mut self.context
    }

    /// Gets a reference to the job context.
    pub fn context(&self) -> &JobContext {
        &self.context
    }

    /// Gets the maximum attempts for a job. Default 25
    pub fn max_attempts(&self) -> i32 {
        self.max_attempts
    }

    /// Records a job attempt
    pub fn record_attempt(&mut self) {
        self.attempts += 1;
    }

    /// Gets the current attempts for a job. Default 0
    pub fn attempts(&self) -> i32 {
        self.attempts
    }
}

impl<J> JobRequest<J>
where
    J: Job,
{
    /// A helper method to executes a [JobRequest] wrapping a [Job]
    pub(crate) async fn do_handle(mut self) -> Result<JobResult, JobError> {
        let id = self.id();
        let (tx, rx) = oneshot::channel();
        self.job.handle(&mut self.context).into_response(Some(tx));
        match rx.await {
            Ok(value) => {
                log::debug!("JobTX [{}] completed with value: {:?}", id, value);
                value
            }
            Err(err) => {
                log::warn!("JobTX [{}] panicked with error: {:?}", id, err);
                Err(JobError::Failed(Box::new(err)))
            }
        }
    }
}

impl<T> std::ops::Deref for JobRequest<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.job
    }
}

impl<'r, T: DeserializeOwned> FromRow<'r, SqliteRow> for JobRequest<T> {
    fn from_row(row: &'r SqliteRow) -> Result<Self, sqlx::Error> {
        let job: String = row.try_get("job")?;
        let id = row.try_get("id")?;
        let run_at: i32 = row.try_get("run_at")?;
        let run_at = Utc.timestamp(run_at.into(), 0);
        let attempts = row.try_get("attempts").unwrap_or_else(|_| 0);
        let max_attempts = row.try_get("max_attempts").unwrap_or_else(|_| 25);
        let done_at: Option<i32> = row.try_get("done_at").unwrap_or_default();
        let lock_at: Option<i32> = row.try_get("lock_at").unwrap_or_default();
        let last_error = row.try_get("last_error").unwrap_or_default();
        let status: String = row.try_get("status")?;
        let lock_by: Option<String> = row.try_get("lock_by").unwrap_or_default();
        Ok(JobRequest {
            job: serde_json::from_str(&job).unwrap(),
            id,
            run_at,
            status: status.parse().unwrap(),
            attempts,
            max_attempts,
            last_error,
            lock_at: lock_at.map(|time| Utc.timestamp(time.into(), 0)),
            lock_by,
            done_at: done_at.map(|time| Utc.timestamp(time.into(), 0)),
            context: JobContext::new(),
        })
    }
}
