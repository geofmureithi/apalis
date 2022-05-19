use std::time::Duration;

use apalis::{
    layers::{tracing::TraceLayer, RateLimitLayer},
    postgres::PostgresStorage,
    Job, JobContext, JobFuture, JobHandler, Monitor, Storage, WorkerBuilder,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Email {
    to: String,
    subject: String,
    text: String,
}

impl Job for Email {
    const NAME: &'static str = "postgres::Email";
}

async fn email_service(email: Email, ctx: JobContext) -> Result<JobResult, JobError> {
    actix::clock::sleep(Duration::from_secs(3)).await;

    ctx.update_progress(42);

    tracing::debug!(subject = ?email.subject, "sending.email");

    actix::clock::sleep(Duration::from_secs(2)).await;

    tracing::debug!(subject = ?email.subject, "sent.email");

    Ok(JobResult::Success)
}

async fn produce_jobs(storage: &SqliteStorage<Email>) {
    let mut storage = storage.clone();
    for i in 0..100 {
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

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "debug,sqlx::query=error");
    tracing_subscriber::fmt::init();

    let pg: PostgresStorage<Email> = PostgresStorage::connect("sqlite://data.db").await.unwrap();
    pg.setup()
        .await
        .expect("unable to run migrations for postgres");

    // This can be in another part of the program
    // produce_jobs(&sqlite).await;

    Monitor::new()
        .register_with_count(1, move |_| {
            WorkerBuilder::new(sqlite.clone())
                .layer(RateLimitLayer::new(10, Duration::from_millis(10)))
                .layer(TraceLayer::new())
                .build()
                .start()
        })
        .run()
        .await
}
