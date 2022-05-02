use std::time::Duration;

use apalis::{
    layers::{
        extensions::Extension,
        retry::{DefaultRetryPolicy, RetryLayer},
    },
    redis::RedisStorage,
    Heartbeat, JobError, JobRequest, JobResult, QueueBuilder, Storage, Worker,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Email {
    to: String,
    subject: String,
    text: String,
}

async fn email_service(job: JobRequest<Email>) -> Result<JobResult, JobError> {
    let attempts = job.attempts();
    println!("Attempting #({}) to send email to {}", attempts, job.to);

    // Get your state here
    let email_state: Option<&EmailState> = job.context().data_opt();
    assert!(email_state.is_some());

    Err(JobError::Unknown)
}

async fn produce_jobs(mut storage: RedisStorage<Email>) {
    storage
        .push(Email {
            to: "test@example.com".to_string(),
            text: "Test backround job from Apalis".to_string(),
            subject: "Background email job".to_string(),
        })
        .await
        .unwrap();
}

#[derive(Clone)]
struct EmailState {
    // ommited
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    let storage = RedisStorage::new("redis://127.0.0.1/").await.unwrap();
    //This can be in another part of the program
    produce_jobs(storage.clone()).await;

    Worker::new()
        .register_with_count(2, move || {
            QueueBuilder::new(storage.clone())
                .layer(RetryLayer::new(DefaultRetryPolicy))
                .layer(Extension(EmailState {}))
                .fetch_interval(Duration::from_millis(50))
                .heartbeat(Heartbeat::EnqueueScheduled(10), Duration::from_millis(50))
                .build_fn(email_service)
                .start()
        })
        .run()
        .await
}
