use apalis::{
    layers::retry::{DefaultRetryPolicy, RetryLayer},
    sqlite::SqliteStorage,
    Job, JobContext, JobError, QueueBuilder, Storage, Worker,
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

    fn handle(self, _ctx: &JobContext) -> Self::Result {
        println!("Sent email to {}", self.to);
        Ok(())
    }
}

async fn produce_jobs(storage: &SqliteStorage<Email>) {
    let mut storage = storage.clone();
    for i in 0..10 {
        storage
            .push(Email {
                to: format!("test{}@example.com", i),
                text: "Test backround job from Apalis".to_string(),
                subject: "Background email job".to_string(),
            })
            .await
            .unwrap();
    }
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "warn");
    env_logger::init();

    let sqlite = SqliteStorage::new("sqlite::memory:").await.unwrap();
    // Do migrations: Mainly for "sqlite::memory:"
    sqlite.setup().await;

    // This can be in another part of the program
    produce_jobs(&sqlite).await;

    Worker::new()
        .register_with_count(1, move || {
            QueueBuilder::new(sqlite.clone())
                .layer(RetryLayer::new(DefaultRetryPolicy))
                .build()
                .start()
        })
        // .event_handler(|e| println!("Handled {:?}", e))
        .run()
        .await
}
