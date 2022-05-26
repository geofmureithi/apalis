use chrono::Utc;
use std::error::Error;
use std::fmt;
use std::time::Duration;
use tracing::{span::Record, Id, Instrument, Span};
use tracing_subscriber::prelude::*;

use actix::clock::sleep;
use apalis::{
    layers::{sentry::SentryJobLayer, tracing::TraceLayer},
    redis::RedisStorage,
    Job, JobContext, JobError, JobResult, Monitor, OnProgress, Storage, TracingOnProgress,
    WorkerBuilder, WorkerPulse,
};
use serde::{Deserialize, Serialize};
use tracing::Subscriber;
use tracing_subscriber::{registry, Layer};

#[derive(Debug, Deserialize, Serialize)]

struct Email {
    to: String,
    subject: String,
    text: String,
}

impl Job for Email {
    const NAME: &'static str = "sentry::Email";
}

#[derive(Debug)]
struct InvalidEmailError {
    email: String,
}

impl fmt::Display for InvalidEmailError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UnknownEmail: {} is not a valid email", self.email)
    }
}

impl Error for InvalidEmailError {}

async fn email_service(email: Email, ctx: JobContext) -> Result<JobResult, JobError> {
    let handle = ctx.get_progress_handle::<TracingOnProgress>().unwrap();
    tracing::info!("Checking if dns configured");
    sleep(Duration::from_millis(1008)).await;
    handle.update_progress(20);
    tracing::info!("Trace!");

    Ok(JobResult::Success)
}

async fn produce_jobs(mut storage: RedisStorage<Email>) {
    storage
        .push(Email {
            to: "test@example".to_string(),
            text: "Test backround job from Apalis".to_string(),
            subject: "Welcome Sentry Email".to_string(),
        })
        .await
        .unwrap();
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    use tracing_subscriber::EnvFilter;
    std::env::set_var("RUST_LOG", "debug");

    let redis_url =
        std::env::var("REDIS_URL").expect("Please set REDIS_URL environmental variable");

    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("debug"))
        .unwrap();
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    let storage = RedisStorage::connect(redis_url)
        .await
        .expect("Could not connect to RedisStorage");
    //This can be in another part of the program
    produce_jobs(storage.clone()).await;

    Monitor::new()
        .register_with_count(2, move |_| {
            WorkerBuilder::new(storage.clone())
                .layer(TraceLayer::new())
                .build_fn(email_service)
                .start()
        })
        .run()
        .await
}
