use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

// Use a wrapper type when adding data via Extension when working on common types.
#[derive(Debug, Clone)]
pub struct ValidEmailCache(Arc<Mutex<HashMap<String, bool>>>);

impl ValidEmailCache {
    pub fn new() -> Self {
        Self(Arc::default())
    }

    pub fn get(&self, key: &str) -> Option<bool> {
        self.0.try_lock().unwrap().get(key).cloned()
    }
}

pub async fn fetch_validity(email_to: String, cache: &ValidEmailCache) -> bool {
    cache.0.try_lock().unwrap().insert(email_to, true);
    tokio::time::sleep(Duration::from_secs(1)).await;
    true
}
