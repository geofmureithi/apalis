use apalis::{
    sqlite::SqliteStorage, JobError, JobRequest, JobResult, QueueBuilder, Storage, Worker,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct Email {
    to: String,
    subject: String,
    text: String,
}

async fn email_service(job: JobRequest<Email>) -> Result<JobResult, JobError> {
    // Do something awesome
    println!("Attempting to send email to {}", job.to);
    Ok(JobResult::Success)
}

async fn produce_jobs(storage: &SqliteStorage<Email>) {
    let mut storage = storage.clone();
    storage
        .push(Email {
            to: "test@example.com".to_string(),
            text: "Test backround job from Apalis".to_string(),
            subject: "Background email job".to_string(),
        })
        .await
        .unwrap();
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();

    let sqlite = SqliteStorage::new("sqlite::memory:").await.unwrap();
    // Do migrations: Mainly for "sqlite::memory:"
    sqlite.setup().await;

    // This can be in another part of the program
    produce_jobs(&sqlite).await;

    Worker::new()
        .register_with_count(2, move || {
            QueueBuilder::new(sqlite.clone())
                .build_fn(email_service)
                .start()
        })
        .run()
        .await
}
