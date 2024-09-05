use anyhow::Result;
use apalis::layers::catch_panic::CatchPanicLayer;
use apalis::utils::TokioExecutor;
use apalis::{layers::tracing::TraceLayer, prelude::*};
use apalis_sql::sqlite::SqliteStorage;

use email_service::Email;
use sqlx::SqlitePool;

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

    Monitor::<TokioExecutor>::new()
        .register_with_count(2, {
            WorkerBuilder::new("tasty-banana")
                .layer(CatchPanicLayer::new())
                .layer(TraceLayer::new())
                .backend(email_storage)
                .build_fn(send_email)
        })
        .on_event(|e| tracing::info!("{e:?}"))
        .run()
        .await?;
    Ok(())
}
