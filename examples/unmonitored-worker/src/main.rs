use std::time::Duration;

use apalis::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SelfMonitoringJob {
    id: i32,
}

async fn self_monitoring_task(task: SelfMonitoringJob, worker: WorkerContext) {
    info!("task: {:?}, {:?}", task, worker);
    if task.id == 99 {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if !worker.has_pending_tasks() {
                    info!("done with all tasks, stopping worker");
                    worker.stop().unwrap();
                    break;
                }
            }
        });
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
}

async fn produce_jobs(storage: &mut MemoryStorage<SelfMonitoringJob>) {
    for id in 0..100 {
        storage.push(SelfMonitoringJob { id }).await.unwrap();
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    unsafe {
        std::env::set_var("RUST_LOG", "debug");
    }
    tracing_subscriber::fmt::init();
    let mut backend = MemoryStorage::new();
    produce_jobs(&mut backend).await;

    WorkerBuilder::new("tasty-banana")
        .backend(backend)
        .enable_tracing()
        .concurrency(20)
        .on_event(|_c, e| info!("{e}"))
        .build(self_monitoring_task)
        .run_until(tokio::signal::ctrl_c()) // Graceful shutdown on Ctrl+C
        .await?;
    Ok(())
}
