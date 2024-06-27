use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Notification {
    pub to: String,
    pub text: String,
}

pub async fn notify(job: Notification) {
    tracing::info!("Attempting to send notification to {}", job.to);
}
