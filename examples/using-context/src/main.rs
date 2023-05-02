use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result;
use apalis::{
    layers::{Extension, TraceLayer},
    prelude::*,
    sqlite::SqliteStorage,
};

use email_service::Email;
use tracing::{Instrument, Span};

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
        tracing::info!(
            "Sending email {email:?} using the reused client: {:?}",
            self.client
        );
    }
}

#[derive(Debug, Clone)]
struct ValidEmailCache(Arc<Mutex<HashMap<String, bool>>>);

async fn fetch_validity(email_to: String, cache: ValidEmailCache) -> bool {
    tokio::time::sleep(Duration::from_secs(5)).await;
    cache.0.try_lock().unwrap().insert(email_to, true);
    true
}

/// Quick solution to prevent spam.
/// If email in cache, then send email else complete the job but let a validation process run in the background,
async fn send_email(email: Email, ctx: JobContext) -> anyhow::Result<()> {
    let email_service = ctx.data::<EmailService>()?.clone();
    let cache = ctx.data::<ValidEmailCache>()?.clone();
    let worker_ctx = ctx.data::<WorkerContext<()>>()?.clone();
    let cache_clone = cache.clone();
    let email_to = email.to.clone();
    let res = cache.0.try_lock().unwrap().get(&email_to).cloned();
    match res {
        None => {
            //We may not prioritize or care when the email is not in cache
            worker_ctx.spawn(
                async move {
                    if fetch_validity(email_to, cache_clone.clone()).await {
                        email_service.send(email).await;
                    }
                }
                .instrument(Span::current()),
            );
        }

        Some(_) => {
            email_service.send(email).await;
        }
    }

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
                .layer(Extension(ValidEmailCache(Arc::default())))
                .with_storage(sqlite.clone())
                .build_fn(send_email)
        })
        .run()
        .await?;
    Ok(())
}
