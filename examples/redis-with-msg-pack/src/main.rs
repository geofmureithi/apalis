use std::{sync::Arc, time::Duration};

use anyhow::Result;
use apalis::prelude::*;
use apalis_redis::RedisStorage;

use apalis_redis::ConnectionManager;
use email_service::{send_email, Email};
use serde::{Deserialize, Serialize};
use tracing::info;

struct MessagePack;

impl Codec for MessagePack {
    type Compact = Vec<u8>;
    type Error = Error;
    fn encode<T: Serialize>(input: T) -> Result<Vec<u8>, Self::Error> {
        rmp_serde::to_vec(&input).map_err(|e| Error::SourceError(Arc::new(Box::new(e))))
    }

    fn decode<O>(compact: Vec<u8>) -> Result<O, Self::Error>
    where
        O: for<'de> Deserialize<'de>,
    {
        rmp_serde::from_slice(&compact).map_err(|e| Error::SourceError(Arc::new(Box::new(e))))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug");

    tracing_subscriber::fmt::init();

    let redis_url = std::env::var("REDIS_URL").expect("Missing env variable REDIS_URL");
    let conn = apalis_redis::connect(redis_url)
        .await
        .expect("Could not connect");
    let config = apalis_redis::Config::default().set_namespace("apalis_redis-with-msg-pack");
    let storage = RedisStorage::new_with_codec::<MessagePack>(conn, config);
    // This can be in another part of the program
    produce_jobs(storage.clone()).await?;

    let worker = WorkerBuilder::new("rango-tango")
        .backend(storage)
        .build_fn(send_email);

    Monitor::new()
        .register(worker)
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

async fn produce_jobs(
    mut storage: RedisStorage<Email, ConnectionManager, MessagePack>,
) -> Result<()> {
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
