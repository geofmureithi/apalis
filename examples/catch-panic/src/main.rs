use anyhow::Result;
use apalis::prelude::*;

use email_service::Email;

async fn produce_emails(storage: &mut MemoryStorage<Email>) -> Result<()> {
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
    unsafe {
        std::env::set_var("RUST_LOG", "debug");
    }
    tracing_subscriber::fmt::init();

    let mut email_storage: MemoryStorage<Email> = MemoryStorage::new();

    produce_emails(&mut email_storage).await?;
    WorkerBuilder::new("tasty-banana")
        .backend(email_storage)
        .enable_tracing()
        .catch_panic()
        .concurrency(2)
        .on_event(|_c, e| tracing::info!("{e:?}"))
        .build(send_email)
        .run()
        .await?;
    Ok(())
}
