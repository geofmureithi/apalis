mod postgres;
mod sqlite;

mod mysql;

pub use mysql::MysqlStorage;

pub use postgres::PostgresStorage;

/// Example
/// ```rust
/// let storage = SqliteStorage::connect(":memory").await.unwrap();
/// ```
pub use sqlite::SqliteStorage;
