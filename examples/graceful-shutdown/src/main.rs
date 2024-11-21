use std::time::Duration;

use apalis::prelude::*;
use apalis_sql::sqlite::{SqlitePool, SqliteStorage};
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Debug, Serialize, Deserialize)]
struct LongRunningJob {}

async fn long_running_task(_task: LongRunningJob, worker: Worker<Context>) {
    loop {
        info!("is_shutting_down: {}", worker.is_shutting_down());
        if worker.is_shutting_down() {
            info!("saving the job state");
            break;
        }
        tokio::time::sleep(Duration::from_secs(3)).await; // Do some hard thing
    }
    info!("Shutdown complete!");
}

async fn produce_jobs(storage: &mut SqliteStorage<LongRunningJob>) {
    storage.push(LongRunningJob {}).await.unwrap();
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    std::env::set_var("RUST_LOG", "debug,sqlx::query=error");
    tracing_subscriber::fmt::init();
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    SqliteStorage::setup(&pool)
        .await
        .expect("unable to run migrations for sqlite");
    let mut sqlite: SqliteStorage<LongRunningJob> = SqliteStorage::new(pool);
    produce_jobs(&mut sqlite).await;
    Monitor::new()
        .register({
            WorkerBuilder::new("tasty-banana")
                .concurrency(2)
                .enable_tracing()
                .backend(sqlite)
                .build_fn(long_running_task)
        })
        .on_event(|e| info!("{e}"))
        // Wait 5 seconds after shutdown is triggered to allow any incomplete jobs to complete
        .shutdown_timeout(Duration::from_secs(5))
        // Use .run() if you don't want without signals
        .run_with_signal(tokio::signal::ctrl_c()) // This will wait for ctrl+c then gracefully shutdown
        .await?;
    Ok(())
}
