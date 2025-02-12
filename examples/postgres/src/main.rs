use anyhow::Result;
use apalis::layers::retry::RetryPolicy;

use apalis::prelude::*;
use apalis_sql::{
    postgres::{PgListen, PgPool, PostgresStorage},
    Config,
};
use email_service::{send_email, Email};
use tracing::{debug, info};

async fn produce_jobs(storage: &mut PostgresStorage<Email>) -> Result<()> {
    for index in 0..10 {
        storage
            .push(Email {
                to: format!("test{}@example.com", index),
                text: "Test background job from apalis".to_string(),
                subject: "Background email job".to_string(),
            })
            .await?;
    }
    // The sql way
    tracing::info!("You can also add jobs via sql query, run this: \n Select apalis.push_job('apalis::Email', json_build_object('subject', 'Test apalis', 'to', 'test1@example.com', 'text', 'Lorem Ipsum'));");
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

    let mut pg = PostgresStorage::new_with_config(pool.clone(), Config::new("apalis::Email"));
    produce_jobs(&mut pg).await?;

    let mut listener = PgListen::new(pool).await?;

    listener.subscribe_with(&mut pg);

    tokio::spawn(async move {
        listener.listen().await.unwrap();
    });

    Monitor::new()
        .register({
            WorkerBuilder::new("tasty-orange")
                .retry(RetryPolicy::retries(5))
                .enable_tracing()
                .backend(pg)
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
