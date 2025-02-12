use std::time::Duration;

use anyhow::Result;
use apalis::layers::ErrorHandlingLayer;
use apalis::prelude::*;
use apalis_redis::RedisStorage;

use email_service::{send_email, Email};
use tracing::{error, info};

async fn produce_jobs(mut storage: RedisStorage<Email>) -> Result<()> {
    for index in 0..10 {
        storage
            .push(Email {
                to: index.to_string(),
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

    let redis_url = std::env::var("REDIS_URL").expect("Missing env variable REDIS_URL");
    let conn = apalis_redis::connect(redis_url)
        .await
        .expect("Could not connect");
    let storage = RedisStorage::new(conn);
    // This can be in another part of the program
    produce_jobs(storage.clone()).await?;

    let worker = WorkerBuilder::new("rango-tango")
        .layer(ErrorHandlingLayer::new())
        .enable_tracing()
        .rate_limit(5, Duration::from_secs(1))
        .timeout(Duration::from_millis(500))
        .concurrency(2)
        .backend(storage)
        .build_fn(send_email);

    Monitor::new()
        .register(worker)
        .on_event(|e| {
            let worker_id = e.id();
            match e.inner() {
                Event::Start => {
                    info!("Worker [{worker_id}] started");
                }
                Event::Error(e) => {
                    error!("Worker [{worker_id}] encountered an error: {e}");
                }

                Event::Exit => {
                    info!("Worker [{worker_id}] exited");
                }
                _ => {}
            }
        })
        .shutdown_timeout(Duration::from_millis(5000))
        .run_with_signal(async {
            info!("Monitor started");
            tokio::signal::ctrl_c().await?;
            info!("Monitor starting shutdown");
            Ok(())
        })
        .await?;
    info!("Monitor shutdown complete");
    Ok(())
}
