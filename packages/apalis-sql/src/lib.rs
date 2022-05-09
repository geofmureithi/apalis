mod sqlite;
//mod postgres;

///
/// ```rust
/// let storage = RedisStorage::new("redis://127.0.0.1/").await.unwrap();
/// ```
pub use sqlite::SqliteStorage;
