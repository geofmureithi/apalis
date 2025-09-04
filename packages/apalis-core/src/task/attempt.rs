//! A thread-safe tracker for counting the number of attempts made by a task.
//!
//! The `Attempt` struct wraps an atomic counter, allowing concurrent increment and retrieval of the attempt count. It is designed to be used within the Apalis job/task system, enabling tasks to keep track of how many times they have been retried or executed.
//!
//! Features:
//! - Thread-safe increment and retrieval of attempt count.
//! - Integration with apalis `FromRequest` trait for extracting attempt information from a task context.
//! - Optional (via the `serde` feature) serialization and deserialization support for persisting or transmitting attempt state.
use std::{
    convert::Infallible,
    sync::{atomic::AtomicUsize, Arc},
};

use crate::{task::Task, task_fn::FromRequest};

/// A wrapper to keep count of the attempts tried by a task
#[derive(Debug, Clone)]
pub struct Attempt(Arc<AtomicUsize>);

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

impl<Args: Sync, Ctx: Sync, IdType: Sync + Send> FromRequest<Task<Args, Ctx, IdType>>
    for Attempt
{
    type Error = Infallible;
    async fn from_request(task: &Task<Args, Ctx, IdType>) -> Result<Self, Self::Error> {
        Ok(task.parts.attempt.clone())
    }
}

#[cfg(feature = "serde")]
mod serde_impl {
    use std::sync::atomic::Ordering;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::*;

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
}
