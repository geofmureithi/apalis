use std::{fmt::Debug, time::Duration};

use apalis::prelude::*;
use apalis_redis::RedisStorage;
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct WelcomeEmail {
    user_id: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]

struct CampaignEmail {
    campaign_id: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]

struct CompleteCampaign {
    completion_id: usize,
}

async fn welcome(req: WelcomeEmail, _ctx: Data<()>) -> Result<GoTo<CampaignEmail>, Error> {
    Ok::<_, _>(GoTo::Next(CampaignEmail {
        campaign_id: req.user_id + 1,
    }))
}

async fn campaign(req: CampaignEmail, _ctx: Data<()>) -> Result<GoTo<CompleteCampaign>, Error> {
    Ok::<_, _>(GoTo::Delay {
        next: CompleteCampaign {
            completion_id: req.campaign_id + 1,
        },
        delay: Duration::from_secs(10),
    })
}

async fn complete_campaign(
    _req: CompleteCampaign,
    _ctx: Data<()>,
) -> Result<GoTo<&'static str>, Error> {
    Ok::<_, _>(GoTo::Done("Completed job successfully"))
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    std::env::set_var("RUST_LOG", "debug,sqlx::query=error");
    tracing_subscriber::fmt::init();
    let conn = apalis_redis::connect("redis://127.0.0.1/").await.unwrap();
    let config = apalis_redis::Config::default().set_namespace("apalis_redis-with-msg-pack");

    let mut storage = RedisStorage::new_with_config(conn, config);
    storage
        .start_stepped(WelcomeEmail { user_id: 1 })
        .await
        .unwrap();

    // Build steps
    let steps = StepBuilder::new()
        .step_fn(welcome)
        .step_fn(campaign)
        .step_fn(complete_campaign);

    WorkerBuilder::new("tasty-banana")
        .data(())
        .enable_tracing()
        .concurrency(2)
        .backend(storage)
        .build_stepped(steps)
        .on_event(|e| info!("{e}"))
        .run()
        .await;
    Ok(())
}
