use std::time::Duration;

use anyhow::Result;
use apalis::prelude::*;
use apalis_redis::RedisStorage;

use email_service::{send_email, Email};
use serde::{de::DeserializeOwned, Serialize};
use tracing::info;

struct MessagePack;

impl<T: Serialize + DeserializeOwned> Codec<T, Vec<u8>> for MessagePack {
    type Error = Error;
    fn encode(&self, input: &T) -> Result<Vec<u8>, Self::Error> {
        rmp_serde::to_vec(input).map_err(|e| Error::SourceError(Box::new(e)))
    }

    fn decode(&self, compact: &Vec<u8>) -> Result<T, Self::Error> {
        rmp_serde::from_slice(compact).map_err(|e| Error::SourceError(Box::new(e)))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug");

    tracing_subscriber::fmt::init();

    let conn = apalis_redis::connect("redis://127.0.0.1/").await?;
    let config = apalis_redis::Config::default()
        .set_namespace("apalis_redis-with-msg-pack")
        .set_max_retries(5);
    let storage = RedisStorage::new_with_codec(conn, config, MessagePack);
    // This can be in another part of the program
    produce_jobs(storage.clone()).await?;

    let worker = WorkerBuilder::new("rango-tango")
        .with_storage(storage)
        .build_fn(send_email);

    Monitor::<TokioExecutor>::new()
        .register_with_count(2, worker)
        .shutdown_timeout(Duration::from_millis(5000))
        .run_with_signal(async {
            tokio::signal::ctrl_c().await?;
            info!("Monitor starting shutdown");
            Ok(())
        })
        .await?;
    info!("Monitor shutdown complete");
    Ok(())
}

async fn produce_jobs(mut storage: RedisStorage<Email>) -> Result<()> {
    for index in 0..10 {
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
