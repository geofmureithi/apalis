use std::time::Duration;

use apalis::{prelude::*, utils::TokioExecutor};
use apalis_sql::sqlite::{SqlitePool, SqliteStorage};
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Debug, Serialize, Deserialize)]
struct LongRunningJob {}

async fn long_running_task(task: LongRunningJob, worker_ctx: Context<TokioExecutor>) {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await; // Do some hard thing
        info!("is_shutting_down: {}", worker_ctx.is_shutting_down(),);
        if worker_ctx.is_shutting_down() {
            info!("saving the job state");
            break;
        }
    }
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
    Monitor::<TokioExecutor>::new()
        .register_with_count(2, {
            WorkerBuilder::new("tasty-banana")
                .backend(sqlite)
                .build_fn(long_running_task)
        })
        // Wait 10 seconds after shutdown is triggered to allow any incomplete jobs to complete
        .shutdown_timeout(Duration::from_secs(10))
        // Use .run() if you don't want without signals
        .run_with_signal(tokio::signal::ctrl_c()) // This will wait for ctrl+c then gracefully shutdown
        .await?;
    Ok(())
}
