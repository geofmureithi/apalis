use apalis::prelude::*;

use apalis_cron::CronStream;
use apalis_cron::Schedule;
use apalis_sql::sqlite::SqliteStorage;
use apalis_sql::sqlx::SqlitePool;
use chrono::DateTime;
use chrono::Local;
use serde::Deserialize;
use serde::Serialize;
use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Reminder(DateTime<Local>);

impl Default for Reminder {
    fn default() -> Self {
        Self(Local::now())
    }
}

async fn send_reminder(job: Reminder) {
    println!("Reminder {:?}", job);
}

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "debug,sqlx::query=error");
    tracing_subscriber::fmt::init();

    // We create our cron jobs stream
    let schedule = Schedule::from_str("1/1 * * * * *").unwrap();
    let cron_stream = CronStream::new(schedule);

    // Lets create a storage for our cron jobs
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    SqliteStorage::setup(&pool)
        .await
        .expect("unable to run migrations for sqlite");
    let sqlite = SqliteStorage::new(pool);

    let backend = cron_stream.pipe_to_storage(sqlite);

    let worker = WorkerBuilder::new("morning-cereal")
        .enable_tracing()
        .rate_limit(1, Duration::from_secs(2))
        .backend(backend)
        .build_fn(send_reminder);
    Monitor::new().register(worker).run().await.unwrap();
}
