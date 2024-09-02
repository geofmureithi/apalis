use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use apalis::{prelude::*, utils::TokioExecutor};
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
    _: SimpleJob,                        // Required, must be of the type of the job/message
    worker_id: WorkerId,                 // The worker running the job, added by worker
    _worker_ctx: Context<TokioExecutor>, // The worker context, added by worker
    _sqlite: Data<SqliteStorage<SimpleJob>>, // The source, added by storage
    task_id: Data<TaskId>,               // The task id, added by storage
    ctx: Data<SqlContext>,               // The task context, added by storage
    count: Data<Count>,                  // Our custom data added via layer
) {
    // increment the counter
    let current = count.fetch_add(1, Ordering::Relaxed);
    info!("worker: {worker_id}; task_id: {task_id:?}, ctx: {ctx:?}, count: {current:?}");
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
    Monitor::<TokioExecutor>::new()
        .register_with_count(2, {
            WorkerBuilder::new("tasty-banana")
                .data(Count::default())
                .backend(sqlite)
                .build_fn(simple_job)
        })
        .run()
        .await?;
    Ok(())
}
