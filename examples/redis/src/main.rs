use std::{
    ops::Deref,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};

use anyhow::Result;
use apalis::prelude::*;
use apalis::redis::RedisStorage;

use email_service::{send_email, Email};
use tracing::{error, info};

async fn produce_jobs(mut storage: RedisStorage<Email>) -> Result<()> {
    for index in 0..100 {
        storage
            .push(Email {
                to: index.to_string(),
                text: "Test background job from apalis".to_string(),
                subject: "Background email job".to_string(),
            })
            .await?;
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
struct Count(Arc<AtomicUsize>);

impl Deref for Count {
    type Target = Arc<AtomicUsize>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug");

    tracing_subscriber::fmt::init();

    let conn = apalis::redis::connect("redis://127.0.0.1/").await?;
    let storage = RedisStorage::new(conn);
    // This can be in another part of the program
    produce_jobs(storage.clone()).await?;

    let worker = WorkerBuilder::new("rango-tango")
        .chain(|svc| svc.timeout(Duration::from_millis(500)))
        .data(Count::default())
        .with_storage(storage)
        .build_fn(send_email);

    Monitor::<TokioExecutor>::new()
        .register_with_count(2, worker)
        .on_event(|e| {
            let worker_id = e.id();
            match e.inner() {
                Event::Start => {
                    info!("Worker [{worker_id}] started");
                }
                Event::Error(e) => {
                    error!("Worker [{worker_id}] encountered an error: {e}");
                }

                Event::Exit => {
                    info!("Worker [{worker_id}] exited");
                }
                _ => {}
            }
        })
        .run_with_signal(async {
            tokio::signal::ctrl_c().await?;
            info!("Monitor starting shutdown");
            Ok(())
        })
        .await?;
    Ok(())
}
