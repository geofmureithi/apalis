use std::time::Duration;

use anyhow::Result;
use apalis::layers::retry::RetryPolicy;

use apalis::prelude::*;
use apalis_sql::{
    postgres::{PgPool, PostgresStorage},
    Config,
};
use email_service::{send_email, Email};
use serde::{Deserialize, Serialize};
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
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or("postgres://postgres:postgres@localhost/postgres".to_owned());

    let pool = PgPool::connect(&database_url).await?;
    PostgresStorage::setup(&pool)
        .await
        .expect("unable to run migrations for postgres");

    let backend = PostgresStorage::new(pool);

    let mut pg = Shared::new(backend);

    let config = Config::new("apalis::Email")
        .set_poll_interval(Duration::from_secs(100))
        .set_buffer_size(1000);

    let mut email_pg = pg.make_shared_with_config(config).unwrap();
    produce_jobs(&mut email_pg).await?;

    #[derive(Debug, Serialize, Deserialize, Clone)]
    struct Sms {
        to: String,
        message: String,
    }

    let sms_pg = pg.make_shared().unwrap();

    Monitor::new()
        .register({
            WorkerBuilder::new("tasty-orange")
                .enable_tracing()
                .backend(email_pg)
                .build_fn(send_email)
        })
        .register({
            WorkerBuilder::new("tasty-sms")
                .enable_tracing()
                .backend(sms_pg)
                .build_fn(|sms: Sms| async move {
                    dbg!(sms);
                })
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
