use std::time::Duration;

use apalis::{
    layers::TraceLayer, mysql::MysqlStorage, Job, JobContext, JobError, JobResult, Monitor,
    Storage, WorkerBuilder, WorkerFactoryFn,
};
use email_service::{send_email, Email};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

async fn produce_jobs(storage: &MysqlStorage<Email>) {
    let mut storage = storage.clone();
    for i in 0..100 {
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

#[tokio::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "debug,sqlx::query=error");
    tracing_subscriber::fmt::init();
    let database_url = std::env::var("DATABASE_URL").expect("Must specify path to db");

    let mysql: MysqlStorage<Email> = MysqlStorage::connect(database_url).await.unwrap();
    // mysql
    //     .setup()
    //     .await
    //     .expect("unable to run migrations for mysql");

    produce_jobs(&mysql).await;
    Monitor::new()
        .register_with_count(2, move |_| {
            WorkerBuilder::new(mysql.clone())
                .layer(TraceLayer::new())
                .build_fn(send_email)
        })
        .run()
        .await
}
