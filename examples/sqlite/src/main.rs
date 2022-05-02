use apalis::{
    layers::retry::{DefaultRetryPolicy, RetryLayer},
    sqlite::SqliteStorage,
    Job, JobContext, JobError, JobRequest, JobResult, QueueBuilder, Storage, Worker,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
struct Email {
    to: String,
    subject: String,
    text: String,
}

#[derive(Clone)]
struct EmailState;

impl Job for Email {
    type Result = Result<(), JobError>;

    fn handle(self, ctx: &mut JobContext) -> Self::Result
    where
        Self: Sized,
    {
        let has_data = ctx.data_opt::<EmailState>();
        println!("Has data b4:{}", has_data.is_some());
        ctx.insert(EmailState);
        let has_data = ctx.data_opt::<EmailState>();
        println!("Has data afr:{}", has_data.is_some());
        Err(JobError::Unknown)
    }
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
    // std::env::set_var("RUST_LOG", "debug");
    // env_logger::init();

    let sqlite = SqliteStorage::new("sqlite::memory:").await.unwrap();
    // Do migrations: Mainly for "sqlite::memory:"
    sqlite.setup().await;

    // This can be in another part of the program
    produce_jobs(&sqlite).await;

    Worker::new()
        .register_with_count(2, move || {
            QueueBuilder::new(sqlite.clone())
                .layer(RetryLayer::new(DefaultRetryPolicy))
                .build()
                .start()
        })
        .run()
        .await
}
