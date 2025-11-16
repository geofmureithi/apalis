use std::sync::Arc;

use email_service::Email;

#[derive(Debug)]
pub struct ExpensiveClient;

#[derive(Debug, Clone)]
pub struct EmailService {
    client: Arc<ExpensiveClient>,
}

impl EmailService {
    pub fn new() -> Self {
        Self {
            client: Arc::new(ExpensiveClient),
        }
    }
    pub async fn send(&self, email: Email) {
        tracing::info!(
            "Sending email {email:?} using the reused client: {:?}",
            self.client
        );
    }
}
