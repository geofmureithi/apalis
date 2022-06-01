use apalis_core::{context::JobContext, request::JobRequest};
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::Row;

/// Wrapper for [JobRequest]
pub(crate) struct SqlJobRequest<T>(JobRequest<T>);

pub(crate) trait IntoJobRequest<T> {
    fn as_job_request(self) -> Option<JobRequest<T>>;
}

impl<T> IntoJobRequest<T> for Option<SqlJobRequest<T>> {
    fn as_job_request(self) -> Option<JobRequest<T>> {
        self.map(|j| j.0)
    }
}

impl<T> Into<JobRequest<T>> for SqlJobRequest<T> {
    fn into(self) -> JobRequest<T> {
        self.0
    }
}

#[cfg(feature = "sqlite")]
#[cfg_attr(docsrs, doc(cfg(feature = "sqlite")))]
impl<'r, T: DeserializeOwned> sqlx::FromRow<'r, sqlx::sqlite::SqliteRow> for SqlJobRequest<T> {
    fn from_row(row: &'r sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
        let job: Value = row.try_get("job")?;
        let id: String = row.try_get("id")?;
        let mut context = JobContext::new(id);

        let run_at = row.try_get("run_at")?;
        context.set_run_at(run_at);

        let attempts = row.try_get("attempts").unwrap_or_else(|_| 0);
        context.set_attempts(attempts);

        let max_attempts = row.try_get("max_attempts").unwrap_or_else(|_| 25);
        context.set_max_attempts(max_attempts);

        let done_at: Option<DateTime<Utc>> = row.try_get("done_at").unwrap_or_default();
        context.set_done_at(done_at);

        let lock_at: Option<DateTime<Utc>> = row.try_get("lock_at").unwrap_or_default();
        context.set_lock_at(lock_at);

        let last_error = row.try_get("last_error").unwrap_or_default();
        context.set_last_error(last_error);

        let status: String = row.try_get("status")?;
        context.set_status(status.parse().unwrap());

        let lock_by: Option<String> = row.try_get("lock_by").unwrap_or_default();
        context.set_lock_by(lock_by);

        Ok(SqlJobRequest(JobRequest::new_with_context(
            serde_json::from_value(job).unwrap(),
            context,
        )))
    }
}

#[cfg(feature = "postgres")]
#[cfg_attr(docsrs, doc(cfg(feature = "postgres")))]
impl<'r, T: DeserializeOwned> sqlx::FromRow<'r, sqlx::postgres::PgRow> for SqlJobRequest<T> {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        let job: Value = row.try_get("job")?;
        let id: String = row.try_get("id")?;
        let mut context = JobContext::new(id);

        let run_at = row.try_get("run_at")?;
        context.set_run_at(run_at);

        let attempts = row.try_get("attempts").unwrap_or_else(|_| 0);
        context.set_attempts(attempts);

        let max_attempts = row.try_get("max_attempts").unwrap_or_else(|_| 25);
        context.set_max_attempts(max_attempts);

        let done_at: Option<DateTime<Utc>> = row.try_get("done_at").unwrap_or_default();
        context.set_done_at(done_at);

        let lock_at: Option<DateTime<Utc>> = row.try_get("lock_at").unwrap_or_default();
        context.set_lock_at(lock_at);

        let last_error = row.try_get("last_error").unwrap_or_default();
        context.set_last_error(last_error);

        let status: String = row.try_get("status")?;
        context.set_status(status.parse().unwrap());

        let lock_by: Option<String> = row.try_get("lock_by").unwrap_or_default();
        context.set_lock_by(lock_by);

        Ok(SqlJobRequest(JobRequest::new_with_context(
            serde_json::from_value(job).unwrap(),
            context,
        )))
    }
}

#[cfg(feature = "mysql")]
#[cfg_attr(docsrs, doc(cfg(feature = "mysql")))]
impl<'r, T: DeserializeOwned> sqlx::FromRow<'r, sqlx::mysql::MySqlRow> for SqlJobRequest<T> {
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> Result<Self, sqlx::Error> {
        let job: Value = row.try_get("job")?;
        let id: String = row.try_get("id")?;
        let mut context = JobContext::new(id);

        let run_at = row.try_get("run_at")?;
        context.set_run_at(run_at);

        let attempts = row.try_get("attempts").unwrap_or_else(|_| 0);
        context.set_attempts(attempts);

        let max_attempts = row.try_get("max_attempts").unwrap_or_else(|_| 25);
        context.set_max_attempts(max_attempts);

        let done_at: Option<DateTime<Utc>> = row.try_get("done_at").unwrap_or_default();
        context.set_done_at(done_at);

        let lock_at: Option<DateTime<Utc>> = row.try_get("lock_at").unwrap_or_default();
        context.set_lock_at(lock_at);

        let last_error = row.try_get("last_error").unwrap_or_default();
        context.set_last_error(last_error);

        let status: String = row.try_get("status")?;
        context.set_status(status.parse().unwrap());

        let lock_by: Option<String> = row.try_get("lock_by").unwrap_or_default();
        context.set_lock_by(lock_by);

        Ok(SqlJobRequest(JobRequest::new_with_context(
            serde_json::from_value(job).unwrap(),
            context,
        )))
    }
}
