use std::time::Duration;

use anyhow::Result;
use apalis::layers::retry::HasherRng;
use apalis::layers::retry::backoff::MakeBackoff;
use apalis::layers::retry::{RetryPolicy, backoff::ExponentialBackoffMaker};

use apalis::prelude::*;
use email_service::{Email, send_email};

/// Produces jobs to check retries
/// See [send_email] for the logic explained here
async fn produce_jobs(storage: &mut MemoryStorage<Email>) -> Result<()> {
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
    unsafe {
        std::env::set_var("RUST_LOG", "debug");
    };
    let mut backend = MemoryStorage::new();
    produce_jobs(&mut backend).await?;
    tracing_subscriber::fmt::init();
    let backoff = ExponentialBackoffMaker::new(
        Duration::from_millis(1000),
        Duration::from_millis(5000),
        1.25,
        HasherRng::default(),
    )?
    .make_backoff();
    WorkerBuilder::new("tasty-orange")
        .backend(backend)
        .retry(
            RetryPolicy::retries(3)
                .with_backoff(backoff)
                .retry_if(|e: &BoxDynError| e.downcast_ref::<AbortError>().is_none()),
        )
        .enable_tracing()
        .build(send_email)
        .run()
        .await?;
    Ok(())
}
