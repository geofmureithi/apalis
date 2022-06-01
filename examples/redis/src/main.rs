use std::time::Duration;

use chrono::Utc;

use apalis::{
    layers::{Extension, TraceLayer},
    redis::{RedisPubSubListener, RedisStorage},
    IntoJobResponse, Job, JobContext, JobError, JobResult, Monitor, Storage, StorageWorkerPulse,
    WorkerBuilder, WorkerFactoryFn,
};
use serde::{Deserialize, Serialize};

use email_service::{send_email, Email};

async fn produce_jobs(mut storage: RedisStorage<Email>) {
    for i in 0..10 {
        storage
            .push(Email {
                to: "test@example.com".to_string(),
                text: "Test backround job from Apalis".to_string(),
                subject: "Background email job".to_string(),
            })
            .await
            .unwrap();
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "debug");

    tracing_subscriber::fmt::init();

    let storage = RedisStorage::connect("redis://127.0.0.1/").await.unwrap();
    //This can be in another part of the program
    produce_jobs(storage.clone()).await;

    let pubsub = RedisPubSubListener::new(storage.get_connection());

    Monitor::new()
        .register(
            WorkerBuilder::new(storage.clone())
                .layer(Extension(storage.clone()))
                .layer(TraceLayer::new())
                .build_fn(send_email),
        )
        .event_handler(pubsub)
        .run()
        .await
}
