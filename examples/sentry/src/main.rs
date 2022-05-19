use chrono::Utc;
use sentry_tower::NewSentryLayer;
use std::error::Error;
use std::fmt;
use std::time::Duration;
use tracing_subscriber::prelude::*;

use actix::clock::sleep;
use apalis::{
    layers::{sentry::SentryJobLayer, tracing::TraceLayer},
    redis::RedisStorage,
    JobContext, JobError, JobResult, Monitor, Storage, WorkerBuilder, WorkerPulse,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]

struct Email {
    to: String,
    subject: String,
    text: String,
}
#[derive(Debug)]
struct InvalidEmailError {
    email: String,
}

impl fmt::Display for InvalidEmailError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InvalidEmailError: {} is not a valid email", self.email)
    }
}

impl Error for InvalidEmailError {}

async fn email_service(email: Email, ctx: JobContext) -> Result<JobResult, JobError> {
    tracing::info!("Checking if dns configured");

    ctx.update_progress(20);
    sleep(Duration::from_millis(100)).await;
    tracing::info!("Getting sendgrid details");
    ctx.update_progress(40);
    sleep(Duration::from_millis(200)).await;
    tracing::info!("Fetching user details");

    ctx.update_progress(65);
    sleep(Duration::from_millis(100)).await;
    tracing::warn!("Digging deeper");

    sleep(Duration::from_millis(200)).await;
    tracing::warn!("Failed. Email is not valid");

    Err(JobError::Failed(Box::from(InvalidEmailError {
        email: email.to,
    })))
}

async fn produce_jobs(mut storage: RedisStorage<Email>) {
    storage
        .schedule(
            Email {
                to: "test@example.com".to_string(),
                text: "Test backround job from Apalis".to_string(),
                subject: "Background email job".to_string(),
            },
            Utc::now() + chrono::Duration::seconds(5),
        )
        .await
        .unwrap();
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    use tracing_subscriber::EnvFilter;
    std::env::set_var("RUST_LOG", "debug");
    let sentry_dsn =
        std::env::var("SENTRY_DSN").expect("Please set SENTRY_DSN environmental variable");
    let redis_url =
        std::env::var("REDIS_URL").expect("Please set REDIS_URL environmental variable");
    let _guard = sentry::init((
        sentry_dsn,
        sentry::ClientOptions {
            release: sentry::release_name!(),
            ..Default::default()
        },
    ));
    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("debug"))
        .unwrap();
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .with(sentry_tracing::layer())
        .init();

    let storage = RedisStorage::connect(redis_url)
        .await
        .expect("Could not connect to RedisStorage");
    //This can be in another part of the program
    produce_jobs(storage.clone()).await;

    Monitor::new()
        .register_with_count(2, move |_| {
            WorkerBuilder::new(storage.clone())
                .layer(NewSentryLayer::new_from_top())
                .layer(SentryJobLayer::new())
                .layer(TraceLayer::new())
                .build_fn(email_service)
                .start()
        })
        .run()
        .await
}
