mod job;

use anyhow::Result;
use apalis::{layers::TraceLayer, prelude::*, sqlite::SqliteStorage};
use chrono::Utc;

use email_service::{send_email, Email};
use job::Notification;
use sqlx::SqlitePool;

async fn produce_emails(storage: &SqliteStorage<Email>) -> Result<()> {
    let mut storage = storage.clone();
    for i in 0..2 {
        storage
            .schedule(
                Email {
                    to: format!("test{i}@example.com"),
                    text: "Test background job from apalis".to_string(),
                    subject: "Background email job".to_string(),
                },
                Utc::now() + chrono::Duration::seconds(i),
            )
            .await?;
    }
    Ok(())
}

async fn produce_notifications(storage: &SqliteStorage<Notification>) -> Result<()> {
    let mut storage = storage.clone();
    for i in 0..2 {
        storage
            .schedule(
                Notification {
                    to: format!("notify:{i}@example.com"),
                    text: "Test background job from apalis".to_string(),
                },
                Utc::now() + chrono::Duration::seconds(i),
            )
            .await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug,sqlx::query=error");
    tracing_subscriber::fmt::init();

    let pool = SqlitePool::connect("sqlite::memory:").await?;

    let email_storage: SqliteStorage<Email> = SqliteStorage::new(pool.clone());
    // Do migrations: Mainly for "sqlite::memory:"
    email_storage
        .setup()
        .await
        .expect("unable to run migrations for sqlite");

    produce_emails(&email_storage).await?;

    let notification_storage: SqliteStorage<Notification> = SqliteStorage::new(pool);

    produce_notifications(&notification_storage).await?;

    Monitor::new()
        .register_with_count(2, move |c| {
            WorkerBuilder::new(format!("tasty-banana-{c}"))
                .layer(TraceLayer::new())
                .with_storage(email_storage.clone())
                .build_fn(send_email)
        })
        .register_with_count(2, move |c| {
            WorkerBuilder::new(format!("tasty-mango-{c}"))
                .layer(TraceLayer::new())
                .with_storage(notification_storage.clone())
                .build_fn(job::notify)
        })
        .run()
        .await?;
    Ok(())
}
