use std::time::Duration;

use anyhow::Result;
use apalis::{layers::TraceLayer, prelude::*, redis::RedisStorage};
use email_service::{send_email, Email};

async fn produce_jobs(mut storage: RedisStorage<Email>) -> Result<()> {
    for _i in 0..1 {
        storage
            .push(Email {
                to: "test@example.com".to_string(),
                text: "Test background job from apalis".to_string(),
                subject: "Background email job".to_string(),
            })
            .await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug");

    tracing_subscriber::fmt::init();

    let storage = RedisStorage::connect("redis://127.0.0.1/").await?;
    //This can be in another part of the program
    produce_jobs(storage.clone()).await?;

    Monitor::new()
        .register(
            WorkerBuilder::new("tasty-guava")
                .layer(TraceLayer::new())
                .with_storage_config(storage, |cfg| {
                    cfg
                        // Reenqueue 100 jobs that are orphaned every second
                        .reenqueue_orphaned(Some((100, Duration::from_secs(1))))
                        // Fetch up to 10 jobs every call to database.
                        .buffer_size(10)
                        // Control how many jobs are moved from the scheduled queue to active queue per interval
                        // Set to None if not using scheduled jobs
                        .enqueue_scheduled(Some((100, Duration::from_secs(1))))
                })
                .build_fn(send_email),
        )
        .run()
        .await?;
    Ok(())
}
