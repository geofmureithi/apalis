use std::{fmt::Debug, time::Duration};

use apalis::{
    layers::{retry::RetryPolicy, tracing::TraceLayer},
    prelude::*,
};
use apalis_core::codec::json::JsonCodec;
use apalis_redis::{RedisContext, RedisStorage};
use serde::{Deserialize, Serialize};
use tower::ServiceBuilder;
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

async fn welcome(req: WelcomeEmail, ctx: Data<()>) -> Result<GoTo<CampaignEmail>, Error> {
    Ok::<_, _>(GoTo::Next(CampaignEmail {
        campaign_id: req.user_id + 1,
    }))
}

async fn campaign(req: CampaignEmail, ctx: Data<()>) -> Result<GoTo<CompleteCampaign>, Error> {
    Ok::<_, _>(GoTo::Delay {
        next: CompleteCampaign {
            completion_id: req.campaign_id + 1,
        },
        delay: Duration::from_secs(10),
    })
}

async fn complete_campaign(
    req: CompleteCampaign,
    ctx: Data<()>,
) -> Result<GoTo<&'static str>, Error> {
    Ok::<_, _>(GoTo::Done("Completed job successfully"))
}

// #[derive(Debug, Hash, PartialEq, Eq, Serialize, Deserialize, Default)]
// enum Steps {
//     #[default]
//     Welcome,
//     Campaign {
//         count: usize,
//     },
//     CompleteCampaign,
//     Done,
// }

// impl StepIndex for Steps {
//     fn next(&self) -> Self {
//         match self {
//             Steps::Welcome => Self::Campaign { count: 0 },
//             Steps::Campaign { count } => {
//                 if *count < 3 {
//                     return Steps::Campaign { count: *count + 1 };
//                 }
//                 Self::CompleteCampaign
//             }
//             Steps::CompleteCampaign => Self::Done,
//             Self::Done => unreachable!(),
//         }
//     }
// }

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    std::env::set_var("RUST_LOG", "debug,sqlx::query=error");
    tracing_subscriber::fmt::init();
    let conn = apalis_redis::connect("redis://127.0.0.1/").await.unwrap();
    let config = apalis_redis::Config::default().set_namespace("apalis_redis-with-msg-pack");

    let mut storage = RedisStorage::new_with_config(conn, config);
    storage
        .start_step(WelcomeEmail { user_id: 1 })
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
