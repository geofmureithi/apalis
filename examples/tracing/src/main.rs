use anyhow::Result;
use apalis::layers::WorkerBuilderExt;
use apalis::prelude::*;
use std::error::Error;
use std::fmt;
use std::time::Duration;
use tracing_subscriber::prelude::*;

use tokio::time::sleep;

use email_service::Email;

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

async fn email_service(email: Email) -> Result<(), InvalidEmailError> {
    tracing::info!("Checking if dns configured");
    sleep(Duration::from_millis(1008)).await;
    tracing::info!("Failed in 1 sec");
    Err(InvalidEmailError { email: email.to })
}

async fn produce_jobs(storage: &mut MemoryStorage<Email>) -> Result<()> {
    storage
        .push(Email {
            to: "test@example".to_string(),
            text: "Test background job from apalis".to_string(),
            subject: "Welcome Sentry Email".to_string(),
        })
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    use tracing_subscriber::EnvFilter;
    std::env::set_var("RUST_LOG", "debug");

    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);
    let filter_layer =
        EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("debug"))?;
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    let mut backend = MemoryStorage::new();
    produce_jobs(&mut backend).await?;

    WorkerBuilder::new("tasty-avocado")
        .backend(backend)
        .enable_tracing()
        .build(email_service)
        .run()
        .await?;
    Ok(())
}
