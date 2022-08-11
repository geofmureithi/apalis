use apalis::{layers::TraceLayer, prelude::*, sqlite::SqliteStorage};
use chrono::Utc;

use email_service::{send_email, Email};

async fn produce_jobs(storage: &SqliteStorage<Email>) {
    let mut storage = storage.clone();
    for i in 0..2 {
        storage
            .schedule(
                Email {
                    to: format!("test{}@example.com", i),
                    text: "Test backround job from Apalis".to_string(),
                    subject: "Background email job".to_string(),
                },
                Utc::now() + chrono::Duration::seconds(i),
            )
            .await
            .unwrap();
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "debug,sqlx::query=error");
    tracing_subscriber::fmt::init();

    let sqlite: SqliteStorage<Email> = SqliteStorage::connect("sqlite://data.db").await.unwrap();
    // Do migrations: Mainly for "sqlite::memory:"
    sqlite
        .setup()
        .await
        .expect("unable to run migrations for sqlite");

    // This can be in another part of the program
    produce_jobs(&sqlite).await;

    Monitor::new()
        .register_with_count(5, move |_| {
            WorkerBuilder::new(sqlite.clone())
                .layer(TraceLayer::new())
                .build_fn(send_email)
        })
        .run()
        .await
}
