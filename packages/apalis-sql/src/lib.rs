mod postgres;
mod sqlite;

pub use postgres::PostgresStorage;

/// Example
/// ```rust
/// let storage = SqliteStorage::connect(":memory").await.unwrap();
/// ```
pub use sqlite::SqliteStorage;
