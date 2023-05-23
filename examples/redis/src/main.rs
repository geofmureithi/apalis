use anyhow::Result;
use apalis::{
    layers::{Extension, TraceLayer},
    prelude::*,
    redis::RedisStorage,
};
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
                .with_storage(storage)
                .build_fn(send_email),
        )
        .run()
        .await?;
    Ok(())
}
