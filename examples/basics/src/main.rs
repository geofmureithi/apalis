mod cache;
mod layer;
mod service;

use std::time::Duration;

use anyhow::Result;
use apalis::{
    layers::{Extension, TraceLayer},
    prelude::*,
    sqlite::SqliteStorage,
};

use email_service::Email;
use layer::LogLayer;

use tracing::{log::info, Instrument, Span};
use tracing_subscriber::registry::Data;

use crate::{cache::ValidEmailCache, service::EmailService};

async fn produce_jobs(storage: &SqliteStorage<Email>) -> Result<()> {
    let mut storage = storage.clone();
    for i in 0..5 {
        storage
            .push(Email {
                to: format!("test{i}@example.com"),
                text: "Test background job from apalis".to_string(),
                subject: "Background email job".to_string(),
            })
            .await?;
        tokio::time::sleep(Duration::from_secs(i)).await;
    }
    Ok(())
}

/// Quick solution to prevent spam.
/// If email in cache, then send email else complete the job but let a validation process run in the background,
async fn send_email(email: Email, srv: Data<EmailService>, worker_ctx: WorkerContext<TokioExecutor>) -> anyhow::Result<()> {
    let svc = ctx.data::<EmailService>()?.clone();
    let cache = ctx.data::<ValidEmailCache>()?.clone();
    let worker_ctx = ctx.data::<WorkerContext<TokioExecutor>>()?;
    info!("Job started in worker {:?}", worker_ctx.id());
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
                    if cache::fetch_validity(email_to, cache_clone.clone()).await {
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
        .register_with_count(2, {
            WorkerBuilder::new(format!("tasty-banana-{c}"))
                .layer(TraceLayer::new())
                // Middleware are executed sequentially
                .layer(LogLayer::new("log-layer-1"))
                .layer(LogLayer::new("log-layer-2"))
                // Add shared context to all jobs executed by this worker
                .layer(Extension(EmailService::new()))
                .layer(Extension(ValidEmailCache::new()))
                // WithStorage is a builder trait that also adds some context eg the storage and worker heartbeats.
                // use .with_storage_config() to configure the storage
                .source(sqlite.clone())
                .build_fn(send_email)
        })
        .shutdown_timeout(Duration::from_secs(5))
        // Use .run() if you don't want without signals
        .run_with_signal(tokio::signal::ctrl_c()) // This will wait for ctrl+c then gracefully shutdown
        .await?;
    Ok(())
}
