use anyhow::Result;

use apalis::prelude::*;
use apalis_sql::mysql::MySqlPool;
use apalis_sql::mysql::MysqlStorage;
use email_service::{send_email, Email};

async fn produce_jobs(storage: &MysqlStorage<Email>) -> Result<()> {
    let mut storage = storage.clone();
    for i in 0..100 {
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

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug,sqlx::query=error");
    tracing_subscriber::fmt::init();
    let database_url = std::env::var("DATABASE_URL").expect("Must specify path to db");
    let pool = MySqlPool::connect(&database_url).await?;

    // Setup migrations
    MysqlStorage::setup(&pool).await?;

    // Create a storage that consumes `Email`
    let mysql: MysqlStorage<Email> = MysqlStorage::new(pool);
    produce_jobs(&mysql).await?;

    Monitor::new()
        .register({
            WorkerBuilder::new("tasty-avocado")
                .enable_tracing()
                .backend(mysql)
                .build_fn(send_email)
        })
        .run()
        .await?;
    Ok(())
}
