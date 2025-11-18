use std::{fmt::Debug, time::Duration};

use apalis::prelude::*;
use apalis_core::backend::json::JsonStorage;
use apalis_workflow::{Workflow, WorkflowSink};
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

async fn welcome(req: WelcomeEmail, _ctx: Data<()>) -> Result<CampaignEmail, BoxDynError> {
    Ok::<_, _>(CampaignEmail {
        campaign_id: req.user_id + 1,
    })
}

async fn campaign(req: CampaignEmail, _ctx: Data<()>) -> Result<CompleteCampaign, BoxDynError> {
    Ok::<_, _>(CompleteCampaign {
        completion_id: req.campaign_id + 1,
    })
}

async fn complete_campaign(_req: CompleteCampaign, _ctx: Data<()>) -> Result<String, BoxDynError> {
    Ok::<_, _>("Completed job successfully".to_string())
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    unsafe {
        std::env::set_var("RUST_LOG", "debug");
    };
    tracing_subscriber::fmt::init();
    let mut backend = JsonStorage::new_temp().unwrap();
    backend
        .push_start(WelcomeEmail { user_id: 1 })
        .await
        .unwrap();

    let workflow = Workflow::new("stepped-workflow")
        .and_then(welcome)
        .delay_for(Duration::from_secs(1))
        .and_then(campaign)
        .and_then(complete_campaign);

    WorkerBuilder::new("tasty-banana")
        .backend(backend)
        .data(())
        .enable_tracing()
        .concurrency(2)
        .on_event(|_c, e| info!("{e}"))
        .build(workflow)
        .run()
        .await?;
    Ok(())
}
