use std::time::Duration;

use chrono::Utc;

use apalis::{
    layers::{extensions::Extension, tracing::TraceLayer},
    redis::{RedisPubSubListener, RedisStorage},
    Job, JobContext, JobError, JobResult, Monitor, Storage, WorkerBuilder, WorkerPulse,
};
use serde::{Deserialize, Serialize};
use tracing::Span;

#[derive(Debug, Deserialize, Serialize)]

struct Email {
    to: String,
    subject: String,
    text: String,
}

impl Job for Email {
    const NAME: &'static str = "redis::Email";
}

async fn email_service(_email: Email, ctx: JobContext) -> Result<JobResult, JobError> {
    // Get your storage here
    let _storage: &RedisStorage<Email> = ctx.data_opt().unwrap();

    Ok(JobResult::Success)
}

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

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "debug");

    tracing_subscriber::fmt::init();

    let storage = RedisStorage::connect("redis://127.0.0.1/").await.unwrap();
    //This can be in another part of the program
    produce_jobs(storage.clone()).await;

    let pubsub = RedisPubSubListener::new(storage.get_connection());

    Monitor::new()
        .register_with_count(4, move |_| {
            WorkerBuilder::new(storage.clone())
                .layer(Extension(storage.clone()))
                .layer(TraceLayer::new())
                .build_fn(email_service)
                .start()
        })
        .event_handler(pubsub)
        .run()
        .await
}
