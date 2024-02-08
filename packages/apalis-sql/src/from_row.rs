use apalis_core::task::task_id::TaskId;
use apalis_core::{data::Extensions, request::Request, worker::WorkerId};
use sqlx::{Decode, Type};

use crate::context::SqlContext;
/// Wrapper for [Request]
#[derive(Debug, Clone)]
pub struct SqlRequest<T> {
    pub(crate) req: T,
    pub(crate) context: SqlContext,
}

impl<T> From<SqlRequest<T>> for Request<T> {
    fn from(val: SqlRequest<T>) -> Self {
        let mut data = Extensions::new();
        data.insert(val.context.id().clone());
        data.insert(val.context.attempts().clone());
        data.insert(val.context);

        Request::new_with_data(val.req, data)
    }
}

#[cfg(feature = "sqlite")]
#[cfg_attr(docsrs, doc(cfg(feature = "sqlite")))]
impl<'r, T: Decode<'r, sqlx::Sqlite> + Type<sqlx::Sqlite>>
    sqlx::FromRow<'r, sqlx::sqlite::SqliteRow> for SqlRequest<T>
{
    fn from_row(row: &'r sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
        use sqlx::types::chrono::DateTime;
        use sqlx::Row;
        use std::str::FromStr;

        let job: T = row.try_get("job")?;
        let id: TaskId =
            TaskId::from_str(row.try_get("id")?).map_err(|e| sqlx::Error::ColumnDecode {
                index: "id".to_string(),
                source: Box::new(e),
            })?;
        let mut context = crate::context::SqlContext::new(id);

        let run_at: i64 = row.try_get("run_at")?;
        context.set_run_at(DateTime::from_timestamp(run_at, 0).unwrap());

        let attempts = row.try_get("attempts").unwrap_or(0);
        context.set_attempts(attempts);

        let max_attempts = row.try_get("max_attempts").unwrap_or(25);
        context.set_max_attempts(max_attempts);

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
        context.set_lock_by(lock_by.map(WorkerId::new));

        Ok(SqlRequest { context, req: job })
    }
}

#[cfg(feature = "postgres")]
#[cfg_attr(docsrs, doc(cfg(feature = "postgres")))]
impl<'r, T: Decode<'r, sqlx::Postgres> + Type<sqlx::Postgres>>
    sqlx::FromRow<'r, sqlx::postgres::PgRow> for SqlRequest<T>
{
    fn from_row(row: &'r sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;
        use std::str::FromStr;
        type Timestamp = i64;

        let job: T = row.try_get("job")?;
        let id: TaskId =
            TaskId::from_str(row.try_get("id")?).map_err(|e| sqlx::Error::ColumnDecode {
                index: "id".to_string(),
                source: Box::new(e),
            })?;
        let mut context = SqlContext::new(id);

        let run_at = row.try_get("run_at")?;
        context.set_run_at(run_at);

        let attempts = row.try_get("attempts").unwrap_or(0);
        context.set_attempts(attempts);

        let max_attempts = row.try_get("max_attempts").unwrap_or(25);
        context.set_max_attempts(max_attempts);

        let done_at: Option<Timestamp> = row.try_get("done_at").unwrap_or_default();
        context.set_done_at(done_at);

        let lock_at: Option<Timestamp> = row.try_get("lock_at").unwrap_or_default();
        context.set_lock_at(lock_at);

        let last_error = row.try_get("last_error").unwrap_or_default();
        context.set_last_error(last_error);

        let status: String = row.try_get("status")?;
        context.set_status(status.parse().map_err(|e| sqlx::Error::ColumnDecode {
            index: "job".to_string(),
            source: Box::new(e),
        })?);

        let lock_by: Option<String> = row.try_get("lock_by").unwrap_or_default();
        context.set_lock_by(lock_by.map(WorkerId::new));
        Ok(SqlRequest { context, req: job })
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

        type Timestamp = i64;

        let job: T = row.try_get("job")?;
        let id: TaskId =
            TaskId::from_str(row.try_get("id")?).map_err(|e| sqlx::Error::ColumnDecode {
                index: "id".to_string(),
                source: Box::new(e),
            })?;
        let mut context = SqlContext::new(id);

        let run_at = row.try_get("run_at")?;
        context.set_run_at(run_at);

        let attempts = row.try_get("attempts").unwrap_or(0);
        context.set_attempts(attempts);

        let max_attempts = row.try_get("max_attempts").unwrap_or(25);
        context.set_max_attempts(max_attempts);

        let done_at: Option<Timestamp> = row.try_get("done_at").unwrap_or_default();
        context.set_done_at(done_at);

        let lock_at: Option<Timestamp> = row.try_get("lock_at").unwrap_or_default();
        context.set_lock_at(lock_at);

        let last_error = row.try_get("last_error").unwrap_or_default();
        context.set_last_error(last_error);

        let status: String = row.try_get("status")?;
        context.set_status(status.parse().map_err(|e| sqlx::Error::ColumnDecode {
            index: "job".to_string(),
            source: Box::new(e),
        })?);

        let lock_by: Option<String> = row.try_get("lock_by").unwrap_or_default();
        context.set_lock_by(lock_by.map(WorkerId::new));

        Ok(SqlRequest { context, req: job })
    }
}
