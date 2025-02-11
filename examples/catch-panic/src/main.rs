use anyhow::Result;
use apalis::prelude::*;

use apalis_sql::{sqlite::SqliteStorage, sqlx::SqlitePool};

use email_service::Email;

async fn produce_emails(storage: &mut SqliteStorage<Email>) -> Result<()> {
    for i in 0..2 {
        storage
            .push(Email {
                to: format!("test{i}@example.com"),
                text: "Test background job from apalis".to_string(),
                subject: "Background email job".to_string(),
            })
            .await?;
    }
    Ok(())
}

async fn send_email(_: Email) {
    unimplemented!("panic from unimplemented")
}

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug,sqlx::query=info");
    tracing_subscriber::fmt::init();

    let pool = SqlitePool::connect("sqlite::memory:").await?;
    // Do migrations: Mainly for "sqlite::memory:"
    SqliteStorage::setup(&pool)
        .await
        .expect("unable to run migrations for sqlite");

    let mut email_storage: SqliteStorage<Email> = SqliteStorage::new(pool.clone());

    produce_emails(&mut email_storage).await?;

    Monitor::new()
        .register({
            WorkerBuilder::new("tasty-banana")
                .catch_panic()
                .enable_tracing()
                .concurrency(2)
                .backend(email_storage)
                .build_fn(send_email)
        })
        .on_event(|e| tracing::info!("{e:?}"))
        .run()
        .await?;
    Ok(())
}
