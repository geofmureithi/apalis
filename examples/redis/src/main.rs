use anyhow::Result;
use apalis::{
    layers::{Extension, TraceLayer},
    prelude::*,
    redis::RedisStorage,
};
use email_service::{send_email, Email};

async fn produce_jobs(mut storage: RedisStorage<Email>) -> Result<()> {
    for _i in 0..10 {
        storage
            .push(Email {
                to: "test@example.com".to_string(),
                text: "Test background job from Apalis".to_string(),
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
            WorkerBuilder::new(storage.clone())
                .layer(Extension(storage.clone()))
                .layer(TraceLayer::new())
                .build_fn(send_email),
        )
        .run()
        .await
}
