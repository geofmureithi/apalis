mod cache;
mod layer;
mod service;

use std::time::Duration;

use apalis::{
    layers::tracing::TraceLayer,
    prelude::*,
    sqlite::{SqlitePool, SqliteStorage},
};

use email_service::Email;
use layer::LogLayer;

use tracing::{log::info, Instrument, Span};

type WorkerCtx = Context<TokioExecutor>;

use crate::{cache::ValidEmailCache, service::EmailService};

async fn produce_jobs(storage: &SqliteStorage<Email>) {
    let mut storage = storage.clone();
    for i in 0..5 {
        storage
            .push(Email {
                to: format!("test{i}@example.com"),
                text: "Test background job from apalis".to_string(),
                subject: "Background email job".to_string(),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(i)).await;
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("data store disconnected")]
    Disconnect(#[from] std::io::Error),
    #[error("the data for key `{0}` is not available")]
    Redaction(String),
    #[error("invalid header (expected {expected:?}, found {found:?})")]
    InvalidHeader { expected: String, found: String },
    #[error("unknown data store error")]
    Unknown,
}

/// Quick solution to prevent spam.
/// If email in cache, then send email else complete the job but let a validation process run in the background,
async fn send_email(
    email: Email,
    svc: Data<EmailService>,
    worker_ctx: Data<WorkerCtx>,
    worker_id: WorkerId,
    cache: Data<ValidEmailCache>,
) -> Result<(), Error> {
    info!("Job started in worker {:?}", worker_id);
    let cache_clone = cache.clone();
    let email_to = email.to.clone();
    let res = cache.get(&email_to);
    match res {
        None => {
            // We may not prioritize or care when the email is not in cache
            // This will run outside the layers scope and after the job has completed.
            // This can be important for starting long running jobs that don't block the queue
            // Its also possible to acquire context types and clone them into the futures context.
            // They will also be gracefully shutdown if [`Monitor`] has a shutdown signal
            worker_ctx.spawn(
                async move {
                    if cache::fetch_validity(email_to, &cache_clone).await {
                        svc.send(email).await;
                        info!("Email added to cache")
                    }
                }
                .instrument(Span::current()), // Its still gonna use the jobs current tracing span. Important eg using sentry.
            );
        }

        Some(_) => {
            svc.send(email).await;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    std::env::set_var("RUST_LOG", "debug,sqlx::query=error");
    tracing_subscriber::fmt::init();
    let pool = SqlitePool::connect("sqlite::memory").await.unwrap();
    SqliteStorage::setup(&pool)
        .await
        .expect("unable to run migrations for sqlite");
    let sqlite: SqliteStorage<Email> = SqliteStorage::new(pool);
    produce_jobs(&sqlite).await;

    Monitor::<TokioExecutor>::new()
        .register_with_count(2, {
            WorkerBuilder::new("tasty-banana".to_string())
                .layer(TraceLayer::new())
                .layer(LogLayer::new("some-log-example"))
                // Add shared context to all jobs executed by this worker
                .data(EmailService::new())
                .data(ValidEmailCache::new())
                .with_storage(sqlite)
                .build_fn(send_email)
        })
        // .shutdown_timeout(Duration::from_secs(5))
        // Use .run() if you don't want without signals
        .run_with_signal(tokio::signal::ctrl_c()) // This will wait for ctrl+c then gracefully shutdown
        .await?;
    Ok(())
}
