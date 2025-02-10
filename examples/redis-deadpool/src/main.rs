use std::time::Duration;

use anyhow::Result;
use apalis::prelude::*;
use apalis_redis::RedisStorage;

use deadpool_redis::{Config, Connection, Runtime};
use email_service::{send_email, Email};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug");

    tracing_subscriber::fmt::init();

    let config = apalis_redis::Config::default().set_namespace("apalis_redis-dead-pool");

    let cfg = Config::from_url("redis://127.0.0.1/");
    let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();
    let conn = pool.get().await.unwrap();
    let mut storage = RedisStorage::new_with_config(conn, config);
    // This can be in another part of the program
    produce_jobs(&mut storage).await?;

    let worker = WorkerBuilder::new("rango-tango")
        .data(pool)
        .backend(storage)
        .build_fn(send_email);

    Monitor::new()
        .register(worker)
        .shutdown_timeout(Duration::from_millis(5000))
        .run_with_signal(async {
            tokio::signal::ctrl_c().await?;
            info!("Monitor starting shutdown");
            Ok(())
        })
        .await?;
    info!("Monitor shutdown complete");
    Ok(())
}

async fn produce_jobs(storage: &mut RedisStorage<Email, Connection>) -> Result<()> {
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
