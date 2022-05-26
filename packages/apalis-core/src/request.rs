use actix::Message;
use chrono::{DateTime, TimeZone, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use sqlx::{mysql::MySqlRow, postgres::PgRow, sqlite::SqliteRow, types::Json, FromRow, Row};
use std::fmt::Debug;
use strum::{AsRefStr, EnumString};
use tokio::sync::oneshot;
use tracing::{Instrument, Level, Span};

use crate::{
    context::JobContext,
    error::JobError,
    error::WorkerError,
    job::{Job, JobHandler},
    response::{JobResponse, JobResult},
};

/// Represents the state of a [JobRequest] in a [Storage]
#[derive(EnumString, Serialize, Deserialize, Debug, Clone, AsRefStr)]
pub enum JobState {
    #[serde(alias = "Latest")]
    Pending,
    Running,
    Done,
    Retry,
    Failed,
    Killed,
}

impl Default for JobState {
    fn default() -> Self {
        JobState::Pending
    }
}

/// A report item for [JobReport]
#[derive(Debug)]
pub enum Report {
    /// This is sent to Queue when a job updates progress
    Progress(u8),
}

/// Represents an update of a job
#[derive(Debug, Message)]
#[rtype(result = "Result<(), WorkerError>")]
pub struct JobReport {
    pub(crate) job_id: String,
    pub(crate) report: Report,
    pub(crate) span: Span, // For tracing purposes
}

/// Represents a job which can be pushed and popped into a [Storage].
///
///
/// Its usually passed to a [JobService] for execution.
#[derive(Serialize, Debug, Deserialize, Clone)]
pub struct JobRequest<T> {
    pub(crate) job: T,
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
    pub(crate) context: JobContext,
}

impl<T> JobRequest<T> {
    /// Creates a new [JobRequest] ready to be pushed to a [Storage]
    pub fn new(job: T) -> Self {
        let id = uuid::Uuid::new_v4().to_string();
        let context = JobContext::new(id.clone());

        Self {
            job,
            id,
            status: JobState::Pending,
            run_at: Utc::now(),
            lock_at: None,
            done_at: None,
            attempts: 0,
            max_attempts: 25,
            last_error: None,
            lock_by: None,
            context,
        }
    }

    /// Get the underlying reference of the [Job]
    pub fn inner(&self) -> &T {
        &self.job
    }

    /// Get the id for a job
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

    /// Set the number of attempts
    pub(crate) fn set_attempts(&mut self, attempts: i32) {
        self.attempts = attempts;
    }

    /// Get the time a job was done
    pub fn done_at(&self) -> &Option<DateTime<Utc>> {
        &self.done_at
    }

    /// Set the time a job was done
    pub(crate) fn set_done_at(&mut self, done_at: DateTime<Utc>) {
        self.done_at = Some(done_at);
    }

    /// Get the time a job was locked
    pub fn lock_at(&self) -> &Option<DateTime<Utc>> {
        &self.lock_at
    }

    pub(crate) fn set_lock_at(&mut self, lock_at: DateTime<Utc>) {
        self.lock_at = Some(lock_at);
    }

    /// Get the job status
    pub fn status(&self) -> &JobState {
        &self.status
    }

    pub(crate) fn set_status(&mut self, status: JobState) {
        self.status = status;
    }

    /// Get the time a job was locked
    pub fn lock_by(&self) -> &Option<String> {
        &self.lock_by
    }

    pub(crate) fn set_lock_by(&mut self, lock_by: String) {
        self.lock_by = Some(lock_by);
    }

    /// Get the time a job was locked
    pub fn last_error(&self) -> &Option<String> {
        &self.last_error
    }

    pub(crate) fn set_last_error(&mut self, error: String) {
        self.last_error = Some(error);
    }
}

impl<J> JobRequest<J>
where
    J: Job + JobHandler<J>,
{
    /// A helper method to executes a [JobRequest] wrapping a [Job]
    pub(crate) async fn do_handle(mut self) -> Result<JobResult, JobError> {
        let (tx, rx) = oneshot::channel();

        let span = Span::current();

        self.job.handle(self.context).into_response(Some(tx));

        let result = match rx.instrument(span).await {
            Ok(value) => {
                tracing::trace!("jobservice.completed");
                value
            }
            Err(err) => {
                tracing::warn!("jobservice.panicked");
                Err(JobError::Failed(Box::new(err)))
            }
        };
        match &result {
            Ok(res) => match res {
                JobResult::Success => {
                    self.status = JobState::Done;
                }
                JobResult::Retry => {
                    self.status = JobState::Retry;
                }
                JobResult::Kill => {
                    self.status = JobState::Killed;
                }
                JobResult::Reschedule(_) => {
                    self.status = JobState::Retry;
                }
            },
            Err(_) => {
                self.status = JobState::Failed;
            }
        };
        result
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
        let id: String = row.try_get("id")?;
        let run_at: i32 = row.try_get("run_at")?;
        let run_at = Utc.timestamp(run_at.into(), 0);
        let attempts = row.try_get("attempts").unwrap_or_else(|_| 0);
        let max_attempts = row.try_get("max_attempts").unwrap_or_else(|_| 25);
        let done_at: Option<i64> = row.try_get("done_at").unwrap_or_default();
        let lock_at: Option<i64> = row.try_get("lock_at").unwrap_or_default();
        let last_error = row.try_get("last_error").unwrap_or_default();
        let status: String = row.try_get("status")?;
        let lock_by: Option<String> = row.try_get("lock_by").unwrap_or_default();
        let context = JobContext::new(id.clone());

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
            context,
        })
    }
}

impl<'r, T: DeserializeOwned> FromRow<'r, PgRow> for JobRequest<T> {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let job: Value = row.try_get("job")?;
        let id: String = row.try_get("id")?;
        let run_at = row.try_get("run_at")?;
        let attempts = row.try_get("attempts").unwrap_or_else(|_| 0);
        let max_attempts = row.try_get("max_attempts").unwrap_or_else(|_| 25);
        let done_at: Option<DateTime<Utc>> = row.try_get("done_at").unwrap_or_default();
        let lock_at: Option<DateTime<Utc>> = row.try_get("lock_at").unwrap_or_default();
        let last_error = row.try_get("last_error").unwrap_or_default();
        let status: String = row.try_get("status")?;
        let lock_by: Option<String> = row.try_get("lock_by").unwrap_or_default();
        let context = JobContext::new(id.clone());
        Ok(JobRequest {
            job: serde_json::from_value(job).unwrap(),
            id,
            run_at,
            status: status.parse().unwrap(),
            attempts,
            max_attempts,
            last_error,
            lock_at,
            lock_by,
            done_at,
            context,
        })
    }
}

impl<'r, T: DeserializeOwned> FromRow<'r, MySqlRow> for JobRequest<T> {
    fn from_row(row: &'r MySqlRow) -> Result<Self, sqlx::Error> {
        let job: Value = row.try_get("job")?;
        let id: String = row.try_get("id")?;
        let run_at = row.try_get("run_at")?;
        let attempts = row.try_get("attempts").unwrap_or_else(|_| 0);
        let max_attempts = row.try_get("max_attempts").unwrap_or_else(|_| 25);
        let done_at: Option<DateTime<Utc>> = row.try_get("done_at").unwrap_or_default();
        let lock_at: Option<DateTime<Utc>> = row.try_get("lock_at").unwrap_or_default();
        let last_error = row.try_get("last_error").unwrap_or_default();
        let status: String = row.try_get("status")?;
        let lock_by: Option<String> = row.try_get("lock_by").unwrap_or_default();
        let context = JobContext::new(id.clone());
        Ok(JobRequest {
            job: serde_json::from_value(job).unwrap(),
            id,
            run_at,
            status: status.parse().unwrap(),
            attempts,
            max_attempts,
            last_error,
            lock_at,
            lock_by,
            done_at,
            context,
        })
    }
}

impl JobReport {
    pub(crate) fn progress(job_id: String, progress: u8) -> JobReport {
        JobReport {
            job_id,
            report: Report::Progress(progress),
            span: Span::current(),
        }
    }
}

pub trait OnProgress {
    fn update_progress(&self, progress: u8);
}

pub struct TracingOnProgress;

impl OnProgress for TracingOnProgress {
    fn update_progress(&self, progress: u8) {
        tracing::event!(target: "job.progress",Level::INFO, progress = progress);
    }
}
