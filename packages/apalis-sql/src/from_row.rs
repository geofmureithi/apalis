use apalis_core::request::Parts;
use apalis_core::task::attempt::Attempt;
use apalis_core::task::task_id::TaskId;
use apalis_core::{request::Request, worker::WorkerId};

use serde::{Deserialize, Serialize};
use sqlx::{Decode, Type};

use crate::context::SqlContext;
/// Wrapper for [Request]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlRequest<T> {
    /// The inner request
    pub req: Request<T, SqlContext>,
    pub(crate) _priv: (),
}

impl<T> SqlRequest<T> {
    /// Creates a new SqlRequest.
    pub fn new(req: Request<T, SqlContext>) -> Self {
        SqlRequest { req, _priv: () }
    }
}

#[cfg(feature = "sqlite")]
#[cfg_attr(docsrs, doc(cfg(feature = "sqlite")))]
impl<'r, T: Decode<'r, sqlx::Sqlite> + Type<sqlx::Sqlite>>
    sqlx::FromRow<'r, sqlx::sqlite::SqliteRow> for SqlRequest<T>
{
    fn from_row(row: &'r sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
        use chrono::DateTime;
        use sqlx::Row;
        use std::str::FromStr;

        let job: T = row.try_get("job")?;
        let task_id: TaskId =
            TaskId::from_str(row.try_get("id")?).map_err(|e| sqlx::Error::ColumnDecode {
                index: "id".to_string(),
                source: Box::new(e),
            })?;
        let mut parts = Parts::<SqlContext>::default();
        parts.task_id = task_id;

        let attempt: i32 = row.try_get("attempts").unwrap_or(0);
        parts.attempt = Attempt::new_with_value(attempt as usize);

        let mut context = crate::context::SqlContext::new();

        let run_at: i64 = row.try_get("run_at")?;
        context.set_run_at(DateTime::from_timestamp(run_at, 0).unwrap_or_default());

        if let Ok(max_attempts) = row.try_get("max_attempts") {
            context.set_max_attempts(max_attempts)
        }

        let done_at: Option<i64> = row.try_get("done_at").unwrap_or_default();
        context.set_done_at(done_at);

        let lock_at: Option<i64> = row.try_get("lock_at").unwrap_or_default();
        context.set_lock_at(lock_at);

        let last_error = row.try_get("last_error").unwrap_or_default();
        context.set_last_error(last_error);

        let status: String = row.try_get("status")?;
        context.set_status(status.parse().map_err(|e| sqlx::Error::ColumnDecode {
            index: "status".to_string(),
            source: Box::new(e),
        })?);

        let lock_by: Option<String> = row.try_get("lock_by").unwrap_or_default();
        context.set_lock_by(
            lock_by
                .as_deref()
                .map(WorkerId::from_str)
                .transpose()
                .map_err(|_| sqlx::Error::ColumnDecode {
                    index: "lock_by".to_string(),
                    source: "Could not parse lock_by as a WorkerId".into(),
                })?,
        );

        let priority: i32 = row.try_get("priority").unwrap_or_default();
        context.set_priority(priority);

        parts.context = context;
        Ok(SqlRequest {
            req: Request::new_with_parts(job, parts),
            _priv: (),
        })
    }
}

#[cfg(feature = "postgres")]
#[cfg_attr(docsrs, doc(cfg(feature = "postgres")))]
impl<'r, T: Decode<'r, sqlx::Postgres> + Type<sqlx::Postgres>>
    sqlx::FromRow<'r, sqlx::postgres::PgRow> for SqlRequest<T>
{
    fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        use chrono::Utc;
        use sqlx::Row;
        use std::str::FromStr;

        let job: T = row.try_get("job")?;
        let task_id: TaskId =
            TaskId::from_str(row.try_get("id")?).map_err(|e| sqlx::Error::ColumnDecode {
                index: "id".to_string(),
                source: Box::new(e),
            })?;
        let mut parts = Parts::<SqlContext>::default();
        parts.task_id = task_id;

        let attempt: i32 = row.try_get("attempts").unwrap_or(0);
        parts.attempt = Attempt::new_with_value(attempt as usize);
        let mut context = SqlContext::new();

        let run_at = row.try_get("run_at")?;
        context.set_run_at(run_at);

        if let Ok(max_attempts) = row.try_get("max_attempts") {
            context.set_max_attempts(max_attempts)
        }

        let done_at: Option<chrono::DateTime<Utc>> = row.try_get("done_at").unwrap_or_default();
        context.set_done_at(done_at.map(|d| d.timestamp()));

        let lock_at: Option<chrono::DateTime<Utc>> = row.try_get("lock_at").unwrap_or_default();
        context.set_lock_at(lock_at.map(|d| d.timestamp()));

        let last_error = row.try_get("last_error").unwrap_or_default();
        context.set_last_error(last_error);

        let status: String = row.try_get("status")?;
        context.set_status(status.parse().map_err(|e| sqlx::Error::ColumnDecode {
            index: "job".to_string(),
            source: Box::new(e),
        })?);

        let lock_by: Option<String> = row.try_get("lock_by").unwrap_or_default();
        context.set_lock_by(
            lock_by
                .as_deref()
                .map(WorkerId::from_str)
                .transpose()
                .map_err(|_| sqlx::Error::ColumnDecode {
                    index: "lock_by".to_string(),
                    source: "Could not parse lock_by as a WorkerId".into(),
                })?,
        );

        let priority: i32 = row.try_get("priority").unwrap_or_default();
        context.set_priority(priority);

        parts.context = context;
        Ok(SqlRequest {
            req: Request::new_with_parts(job, parts),
            _priv: (),
        })
    }
}

#[cfg(feature = "mysql")]
#[cfg_attr(docsrs, doc(cfg(feature = "mysql")))]
impl<'r, T: Decode<'r, sqlx::MySql> + Type<sqlx::MySql>> sqlx::FromRow<'r, sqlx::mysql::MySqlRow>
    for SqlRequest<T>
{
    fn from_row(row: &'r sqlx::mysql::MySqlRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;
        use std::str::FromStr;
        let job: T = row.try_get("job")?;
        let task_id: TaskId =
            TaskId::from_str(row.try_get("id")?).map_err(|e| sqlx::Error::ColumnDecode {
                index: "id".to_string(),
                source: Box::new(e),
            })?;
        let mut parts = Parts::<SqlContext>::default();
        parts.task_id = task_id;

        let attempt: i32 = row.try_get("attempts").unwrap_or(0);
        parts.attempt = Attempt::new_with_value(attempt as usize);

        let mut context = SqlContext::new();

        let run_at = row.try_get("run_at")?;
        context.set_run_at(run_at);

        if let Ok(max_attempts) = row.try_get("max_attempts") {
            context.set_max_attempts(max_attempts)
        }

        let done_at: Option<chrono::NaiveDateTime> = row.try_get("done_at").unwrap_or_default();
        context.set_done_at(done_at.map(|d| d.and_utc().timestamp()));

        let lock_at: Option<chrono::NaiveDateTime> = row.try_get("lock_at").unwrap_or_default();
        context.set_lock_at(lock_at.map(|d| d.and_utc().timestamp()));

        let last_error = row.try_get("last_error").unwrap_or_default();
        context.set_last_error(last_error);

        let status: String = row.try_get("status")?;
        context.set_status(status.parse().map_err(|e| sqlx::Error::ColumnDecode {
            index: "job".to_string(),
            source: Box::new(e),
        })?);

        let lock_by: Option<String> = row.try_get("lock_by").unwrap_or_default();
        context.set_lock_by(
            lock_by
                .as_deref()
                .map(WorkerId::from_str)
                .transpose()
                .map_err(|_| sqlx::Error::ColumnDecode {
                    index: "lock_by".to_string(),
                    source: "Could not parse lock_by as a WorkerId".into(),
                })?,
        );

        let priority: i32 = row.try_get("priority").unwrap_or_default();
        context.set_priority(priority);

        parts.context = context;
        Ok(SqlRequest {
            req: Request::new_with_parts(job, parts),
            _priv: (),
        })
    }
}
