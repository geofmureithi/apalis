use std::time::Duration;

use anyhow::Result;
use apalis::layers::retry::backoff::MakeBackoff;
use apalis::layers::retry::HasherRng;
use apalis::layers::retry::{backoff::ExponentialBackoffMaker, RetryPolicy};

use apalis::prelude::*;
use apalis_sql::{
    postgres::{PgPool, PostgresStorage},
    Config,
};
use email_service::{send_email, Email};
use tracing::{debug, info};

/// Produces jobs to check retries
/// See [send_email] for the logic explained here
async fn produce_jobs(storage: &mut PostgresStorage<Email>) -> Result<()> {
    storage
        .push(Email {
            // Valid email should just run once (attempts = 1)
            to: format!("test{}@example.com", 0),
            text: "Test background job from apalis".to_string(),
            subject: "Background email job".to_string(),
        })
        .await?;
    storage
        .push(Email {
            // Invalid email, should fail and retry 3 times (attempts = 4)
            to: "test.at.example.com".to_owned(),
            text: "Test background job from apalis".to_string(),
            subject: "Background email job".to_string(),
        })
        .await?;
    storage
        .push(Email {
            // Invalid character, job will abort. Should only run once (attempts = 1)
            to: "A@b@c@example.com".to_owned(),
            text: "Test background job from apalis".to_string(),
            subject: "Background email job".to_string(),
        })
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug,sqlx::query=error");
    tracing_subscriber::fmt::init();
    let database_url = std::env::var("DATABASE_URL").expect("Must specify path to db");

    let pool = PgPool::connect(&database_url).await?;
    PostgresStorage::setup(&pool)
        .await
        .expect("unable to run migrations for postgres");

    let mut pg_with_retry =
        PostgresStorage::new_with_config(pool.clone(), Config::new("apalis::Email"));
    let mut pg_with_backoff =
        PostgresStorage::new_with_config(pool.clone(), Config::new("apalis::BackoffEmail"));

    produce_jobs(&mut pg_with_retry).await?;
    produce_jobs(&mut pg_with_backoff).await?;

    Monitor::new()
        .register({
            WorkerBuilder::new("tasty-orange")
                // Ensure this is above tracing layer, else you wont see traces
                .retry(RetryPolicy::retries(3))
                .enable_tracing()
                .backend(pg_with_retry)
                .build_fn(send_email)
        })
        .register({
            let backoff = ExponentialBackoffMaker::new(
                Duration::from_millis(1000),
                Duration::from_millis(5000),
                1.25,
                HasherRng::default(),
            )?
            .make_backoff();

            WorkerBuilder::new("tasty-orange-with-backoff")
                .retry(RetryPolicy::retries(3).with_backoff(backoff))
                .enable_tracing()
                .backend(pg_with_backoff)
                .build_fn(send_email)
        })
        .on_event(|e| debug!("{e}"))
        .run_with_signal(async {
            tokio::signal::ctrl_c().await?;
            info!("Shutting down the system");
            Ok(())
        })
        .await?;
    Ok(())
}
