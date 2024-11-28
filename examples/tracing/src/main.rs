use anyhow::Result;
use apalis::layers::WorkerBuilderExt;
use apalis::prelude::{Monitor, Storage, WorkerBuilder, WorkerFactoryFn};
use apalis_redis::RedisStorage;
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

async fn produce_jobs(mut storage: RedisStorage<Email>) -> Result<()> {
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

    let redis_url =
        std::env::var("REDIS_URL").expect("Please set REDIS_URL environmental variable");

    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);
    let filter_layer =
        EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("debug"))?;
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    let conn = apalis_redis::connect(redis_url)
        .await
        .expect("Could not connect to RedisStorage");
    let storage = RedisStorage::new(conn);
    //This can be in another part of the program
    produce_jobs(storage.clone()).await?;

    Monitor::new()
        .register(
            WorkerBuilder::new("tasty-avocado")
                .enable_tracing()
                .backend(storage)
                .build_fn(email_service),
        )
        .run()
        .await?;
    Ok(())
}
