use apalis::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Notification {
    pub to: String,
    pub text: String,
}

impl Job for Notification {
    const NAME: &'static str = "apalis::Notification";
}

pub async fn notify(job: Notification) {
    tracing::info!("Attempting to send notification to {}", job.to);
}
