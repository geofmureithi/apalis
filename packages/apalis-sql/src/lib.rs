use apalis::Storage;

use sqlx::pool::Pool;
use sqlx::Sqlite;

pub struct SequelStorage<D: sqlx::Database> {
    pool: Pool<D>
}


#[cfg(any(feature = "mysql", feature = "sqlite"))]
impl Storage for SequelStorage<Sqlite> {

    fn push(&self) {
        sqlx::query::<Sqlite>("INSERT INTO jobs (job) VALUES (?)").execute(&self.pool.clone());
    }
    fn fetch(&self) {
        todo!()
    }

    fn ack(&self) {
        todo!()
    }

}

#[cfg(any(feature = "postgres"))]
impl Storage for SequelStorage {

}