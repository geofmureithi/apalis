//! Represents a queue in the backend
//!
//! This module provides the `Queue` struct and related functionality for managing
//! queues in the backend. A queue is identified by its name and is used to group
//! tasks for processing by workers.
//!
//! The `Queue` struct is designed to be lightweight and easily clonable, allowing
//! it to be passed around in various contexts. It uses an `Arc<String>` internally
//! to store the queue name, ensuring efficient memory usage and thread safety.
//!
//! The module also includes an implementation of the `FromRequest` trait, allowing
//! extraction of the queue information from a task context. This is useful for
//! workers that need to know which queue they are processing tasks from.
use std::{str::FromStr, sync::Arc};

use crate::{task::Task, task_fn::FromRequest};

/// Represents a queue in the backend
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Queue(Arc<String>);

impl From<String> for Queue {
    fn from(value: String) -> Self {
        Self(Arc::new(value))
    }
}
impl AsRef<str> for Queue {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Queue {
    fn from(value: &str) -> Self {
        Self(Arc::new(value.to_string()))
    }
}

impl FromStr for Queue {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Arc::new(s.to_string())))
    }
}

impl std::fmt::Display for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Queue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Queue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Queue(Arc::new(s)))
    }
}

impl<Args, Ctx, IdType> FromRequest<Task<Args, Ctx, IdType>> for Queue
where
    Args: Sync,
    Ctx: Sync,
    IdType: Sync + Send,
{
    type Error = QueueError;

    async fn from_request(req: &Task<Args, Ctx, IdType>) -> Result<Self, Self::Error> {
        let tagged = &req.parts.queue;
        match tagged {
            Some(queue) => Ok(queue.clone()),
            None => {
                // Fallback to looking into task data for backward compatibility
                let queue = req
                    .parts
                    .data
                    .get()
                    .cloned()
                    .ok_or_else(|| QueueError::NotFound)?;
                Ok(queue)
            }
        }
    }
}

/// Errors that can occur when extracting queue information from a task context
#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    /// Queue data not found in task context
    #[error("Queue data not found in task context. This is likely a bug. Please report it.")]
    NotFound,
}
