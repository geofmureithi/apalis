use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use apalis::prelude::*;
use apalis_sql::{
    context::SqlContext,
    sqlite::{SqlitePool, SqliteStorage},
};
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Debug, Serialize, Deserialize)]
struct SimpleJob {}

// A task can have up to 16 arguments
async fn simple_job(
    _: SimpleJob,            // Required, must be of the type of the job/message
    worker: Worker<Context>, // The worker and its context, added by worker
    _sqlite: Data<SqliteStorage<SimpleJob>>, // The source, added by storage
    task_id: TaskId,         // The task id, added by storage
    attempt: Attempt,        // The current attempt
    ctx: SqlContext,         // The task context provided by the backend
    count: Data<Count>,      // Our custom data added via layer
) {
    // increment the counter
    let current = count.fetch_add(1, Ordering::Relaxed);
    info!("worker: {worker:?}; task_id: {task_id:?}, ctx: {ctx:?}, attempt:{attempt:?} count: {current:?}");
}

async fn produce_jobs(storage: &mut SqliteStorage<SimpleJob>) {
    for _ in 0..10 {
        storage.push(SimpleJob {}).await.unwrap();
    }
}

#[derive(Clone, Debug, Default)]
struct Count(Arc<AtomicUsize>);

impl Deref for Count {
    type Target = Arc<AtomicUsize>;
    fn deref(&self) -> &Self::Target {
        &self.0
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
    let mut sqlite: SqliteStorage<SimpleJob> = SqliteStorage::new(pool);
    produce_jobs(&mut sqlite).await;
    Monitor::new()
        .register({
            WorkerBuilder::new("tasty-banana")
                .data(Count::default())
                .data(sqlite.clone())
                .concurrency(2)
                .backend(sqlite)
                .build_fn(simple_job)
        })
        .run()
        .await?;
    Ok(())
}
