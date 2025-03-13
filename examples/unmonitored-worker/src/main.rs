use std::time::Duration;

use apalis::prelude::*;
use apalis_sql::sqlite::{SqlitePool, SqliteStorage};
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Debug, Serialize, Deserialize)]
struct SelfMonitoringJob {
    id: i32,
}

async fn self_monitoring_task(task: SelfMonitoringJob, worker: Worker<Context>) {
    info!("task: {:?}, {:?}", task, worker);
    if task.id == 99 {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if !worker.has_pending_tasks() {
                    info!("done with all tasks, stopping worker");
                    worker.stop();
                    break;
                }
            }
        });
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
}

async fn produce_jobs(storage: &mut SqliteStorage<SelfMonitoringJob>) {
    for id in 0..100 {
        storage.push(SelfMonitoringJob { id }).await.unwrap();
    }
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
        .enable_tracing()
        .concurrency(20)
        .backend(sqlite)
        .build(service_fn(self_monitoring_task))
        .on_event(|e| info!("{e}"))
        .run()
        .await;
    Ok(())
}
