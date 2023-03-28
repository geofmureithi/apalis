use anyhow::Result;
use apalis::prelude::*;
use apalis::{layers::TraceLayer, postgres::PostgresStorage};
use email_service::{send_email, Email};

async fn produce_jobs(storage: &PostgresStorage<Email>) -> Result<()> {
    // The programmatic way
    let mut storage = storage.clone();
    storage
        .push(Email {
            to: "test@example.com".to_string(),
            text: "Test background job from Apalis".to_string(),
            subject: "Background email job".to_string(),
        })
        .await?;
    // The sql way
    tracing::info!("You can also add jobs via sql query, run this: \n Select apalis.push_job('apalis::Email', json_build_object('subject', 'Test Apalis', 'to', 'test1@example.com', 'text', 'Lorem Ipsum'));");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug,sqlx::query=error");
    tracing_subscriber::fmt::init();
    let database_url = std::env::var("DATABASE_URL").expect("Must specify path to db");

    let pg: PostgresStorage<Email> = PostgresStorage::connect(database_url).await?;
    pg.setup()
        .await
        .expect("unable to run migrations for postgres");

    produce_jobs(&pg).await?;

    Monitor::new()
        .register_with_count(4, move |c| {
            WorkerBuilder::new(format!("tasty-orange-{c}"))
                .layer(TraceLayer::new())
                .with_storage(pg.clone())
                .build_fn(send_email)
        })
        .run()
        .await?;
    Ok(())
}
