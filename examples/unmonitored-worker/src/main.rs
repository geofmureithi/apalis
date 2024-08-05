use std::time::Duration;

use apalis::{prelude::*, utils::TokioExecutor};
use apalis_sql::sqlite::{SqlitePool, SqliteStorage};
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Debug, Serialize, Deserialize)]
struct SelfMonitoringJob {}

async fn self_monitoring_task(task: SelfMonitoringJob, worker_ctx: Context<TokioExecutor>) {
    info!("task: {:?}, {:?}", task, worker_ctx);
    tokio::time::sleep(Duration::from_secs(5)).await; // Do some hard thing
    info!("done with task, stopping worker gracefully");
    // use worker_ctx.force_stop() to stop immediately
    worker_ctx.stop();
}

async fn produce_jobs(storage: &mut SqliteStorage<SelfMonitoringJob>) {
    storage.push(SelfMonitoringJob {}).await.unwrap();
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    std::env::set_var("RUST_LOG", "debug,sqlx::query=error");
    tracing_subscriber::fmt::init();
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    SqliteStorage::setup(&pool)
        .await
        .expect("unable to run migrations for sqlite");
    let mut sqlite: SqliteStorage<SelfMonitoringJob> = SqliteStorage::new(pool);
    produce_jobs(&mut sqlite).await;

    WorkerBuilder::new("tasty-banana")
        .backend(sqlite)
        .build_fn(self_monitoring_task)
        .with_executor(TokioExecutor)
        .run()
        .await;
    Ok(())
}
