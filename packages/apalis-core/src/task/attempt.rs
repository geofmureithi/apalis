use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{request::Request, service_fn::FromRequest};

/// A wrapper to keep count of the attempts tried by a task
#[derive(Debug, Clone)]
pub struct Attempt(Arc<AtomicUsize>);

// Custom serialization function
fn serialize<S>(attempt: &Attempt, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let value = attempt.0.load(Ordering::SeqCst);
    serializer.serialize_u64(value as u64)
}

// Custom deserialization function
fn deserialize<'de, D>(deserializer: D) -> Result<Attempt, D::Error>
where
    D: Deserializer<'de>,
{
    let value = u64::deserialize(deserializer)?;
    Ok(Attempt(Arc::new(AtomicUsize::new(value as usize))))
}

impl Serialize for Attempt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serialize(self, serializer)
    }
}

impl<'de> Deserialize<'de> for Attempt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize(deserializer)
    }
}

impl Default for Attempt {
    fn default() -> Self {
        Self(Arc::new(AtomicUsize::new(0)))
    }
}

impl Attempt {
    /// Build a new tracker
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a tracker from an existing value
    pub fn new_with_value(value: usize) -> Self {
        Self(Arc::new(AtomicUsize::from(value)))
    }

    /// Get the current value
    pub fn current(&self) -> usize {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Increase the current value
    pub fn increment(&self) -> usize {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

impl<Req, Ctx> FromRequest<Request<Req, Ctx>> for Attempt {
    fn from_request(req: &Request<Req, Ctx>) -> Result<Self, crate::error::Error> {
        Ok(req.parts.attempt.clone())
    }
}
