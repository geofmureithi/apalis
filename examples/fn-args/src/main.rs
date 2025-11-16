use std::{
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use apalis::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SimpleJob {}

// A task can have up to 16 arguments that implement `FromRequest`
async fn simple_job(
    _: SimpleJob, // Required, must be of the type of the job/message and the first argument
    worker: WorkerContext, // The worker and its context, added by worker
    task_id: TaskId, // The task id, added by storage
    attempt: Attempt, // The current attempt
    count: Data<Count>, // Our custom data added via layer
) {
    // increment the counter
    let current = count.fetch_add(1, Ordering::Relaxed);
    info!("worker: {worker:?}; task_id: {task_id:?}, attempt:{attempt:?} count: {current:?}");
}

async fn produce_jobs(storage: &mut MemoryStorage<SimpleJob>) {
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
async fn main() -> Result<(), WorkerError> {
    unsafe {
        std::env::set_var("RUST_LOG", "debug");
    }
    tracing_subscriber::fmt::init();
    let mut backend = MemoryStorage::new();
    produce_jobs(&mut backend).await;
    WorkerBuilder::new("tasty-banana")
        .backend(backend)
        .enable_tracing()
        .data(Count::default())
        .build(simple_job)
        .run()
        .await?;
    Ok(())
}
