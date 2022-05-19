use std::time::Duration;

use chrono::Utc;

use apalis::{
    layers::{extensions::Extension, tracing::TraceLayer},
    redis::RedisStorage,
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

async fn email_service(email: Email, ctx: JobContext) -> Result<JobResult, JobError> {
    // Get your storage here
    let _storage: &RedisStorage<Email> = ctx.data_opt().unwrap();

    actix::clock::sleep(Duration::from_secs(3)).await;

    ctx.update_progress(99);

    tracing::debug!(subject = ?email.subject, "sending.email");

    actix::clock::sleep(Duration::from_secs(2)).await;

    tracing::debug!(subject = ?email.subject, "sent.email");

    Ok(JobResult::Success)
}

async fn produce_jobs(mut storage: RedisStorage<Email>) {
    storage
        .schedule(
            Email {
                to: "test@example.com".to_string(),
                text: "Test backround job from Apalis".to_string(),
                subject: "Background email job".to_string(),
            },
            Utc::now() + chrono::Duration::seconds(1),
        )
        .await
        .unwrap();
}

fn on_success(_: &JobResult, duration: Duration, _: &Span) {
    tracing::info!(done_in = ?duration,  "email.sent")
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "debug");

    tracing_subscriber::fmt::init();

    let storage = RedisStorage::connect("redis://127.0.0.1/").await.unwrap();
    //This can be in another part of the program
    produce_jobs(storage.clone()).await;

    Monitor::new()
        .register_with_count(1, move |_| {
            WorkerBuilder::new(storage.clone())
                .layer(Extension(storage.clone()))
                .layer(TraceLayer::new().on_response(on_success))
                .build_fn(email_service)
                .start()
        })
        .run()
        .await
}
