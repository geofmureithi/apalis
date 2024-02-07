mod job;

use anyhow::Result;
use apalis::utils::TokioExecutor;
use apalis::{layers::tracing::TraceLayer, prelude::*, sqlite::SqliteStorage};
use chrono::Utc;

use email_service::{send_email, Email};
use job::Notification;
use sqlx::SqlitePool;

async fn produce_emails(storage: &SqliteStorage<Email>) -> Result<()> {
    let mut storage = storage.clone();
    for i in 0..1 {
        storage
            .schedule(
                Email {
                    to: format!("test{i}@example.com"),
                    text: "Test background job from apalis".to_string(),
                    subject: "Background email job".to_string(),
                },
                (Utc::now() + chrono::Duration::seconds(4)).timestamp(),
            )
            .await?;
    }
    Ok(())
}

async fn produce_notifications(storage: &SqliteStorage<Notification>) -> Result<()> {
    let mut storage = storage.clone();
    for i in 0..20 {
        storage
            .push(Notification {
                to: format!("notify:{i}@example.com"),
                text: "Test background job from apalis".to_string(),
            })
            .await?;
    }
    Ok(())
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

    let email_storage: SqliteStorage<Email> = SqliteStorage::new(pool.clone());

    produce_emails(&email_storage).await?;

    let notification_storage: SqliteStorage<Notification> = SqliteStorage::new(pool);

    produce_notifications(&notification_storage).await?;

    Monitor::<TokioExecutor>::new()
        .register_with_count(2, {
            WorkerBuilder::new("tasty-banana")
                .layer(TraceLayer::new())
                .with_storage(email_storage)
                .build_fn(send_email)
        })
        .register_with_count(10, {
            WorkerBuilder::new("tasty-mango")
                .layer(TraceLayer::new())
                .with_storage(notification_storage)
                .build_fn(job::notify)
        })
        .run()
        .await?;
    Ok(())
}
