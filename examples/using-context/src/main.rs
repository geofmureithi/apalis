use std::sync::Arc;

use anyhow::Result;
use apalis::{layers::{TraceLayer, Extension}, prelude::*, sqlite::SqliteStorage};

use email_service::Email;

async fn produce_jobs(storage: &SqliteStorage<Email>) -> Result<()> {
    let mut storage = storage.clone();
    storage
        .push(Email {
            to: format!("test@example.com"),
            text: "Test background job from apalis".to_string(),
            subject: "Background email job".to_string(),
        })
        .await?;
    Ok(())
}

#[derive(Debug)]
struct ExpensiveClient;

#[derive(Debug, Clone)]
struct EmailService {
    client: Arc<ExpensiveClient>,
}

impl EmailService {
    fn new() -> Self {
        Self {
            client: Arc::new(ExpensiveClient),
        }
    }
    async fn send(&self, email: Email) {
        tracing::info!("Sending email {email:?} using the reused client: {:?}", self.client);
    }
}

async fn send_email(email: Email, ctx: JobContext) -> anyhow::Result<()> {
    let email_service: &EmailService = ctx.data()?;
    email_service.send(email).await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug,sqlx::query=error");
    tracing_subscriber::fmt::init();

    let sqlite: SqliteStorage<Email> = SqliteStorage::connect("sqlite::memory:").await?;
    sqlite
        .setup()
        .await
        .expect("unable to run migrations for sqlite");

    produce_jobs(&sqlite).await?;

    Monitor::new()
        .register_with_count(2, move |c| {
            WorkerBuilder::new(format!("tasty-banana-{c}"))
                .layer(TraceLayer::new())
                .layer(Extension(EmailService::new()))
                .with_storage(sqlite.clone())
                .build_fn(send_email)
        })
        .run()
        .await?;
    Ok(())
}
