//! Event definitions and utility types for worker events
//!
//! The `Event` enum defines various events that can occur during the lifecycle of a worker, such as starting, stopping, idling, and encountering errors.

use std::{
    any::Any,
    fmt,
    sync::{Arc, RwLock},
};

use crate::{error::BoxDynError, worker::context::WorkerContext};

/// An event handler for a worker
pub type EventHandlerBuilder =
    Arc<RwLock<Option<Box<dyn Fn(&WorkerContext, &Event) + Send + Sync>>>>;

/// Type alias for an event listener function wrapped in an `Arc`
pub type EventListener = Arc<RawEventListener>;

/// Event listening type
pub(crate) type RawEventListener = Box<dyn Fn(&WorkerContext, &Event) + Send + Sync>;

/// Events emitted by a worker
#[derive(Debug)]
pub enum Event {
    /// Worker started
    Start,
    /// Worker is idle, stream has no new request for now
    Idle,
    /// Worker did a heartbeat
    HeartBeat,
    /// A custom event
    Custom(Box<dyn Any + 'static + Send + Sync>),
    /// A result of processing
    Success,
    /// Worker encountered an error
    Error(Arc<BoxDynError>),
    /// Worker stopped
    Stop,
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let event_description = match &self {
            Self::Start => "Worker started".to_owned(),
            Self::Idle => "Worker is idle".to_owned(),
            Self::Custom(_) => "Custom event".to_owned(),
            Self::Error(err) => format!("Worker encountered an error: {err}"),
            Self::Stop => "Worker stopped".to_owned(),
            Self::HeartBeat => "Worker Heartbeat".to_owned(),
            Self::Success => "Worker completed task successfully".to_owned(),
        };

        write!(f, "WorkerEvent: {event_description}")
    }
}

impl Event {
    /// If the event is an error, return the error
    #[must_use]
    pub fn as_error(&self) -> Option<Arc<BoxDynError>> {
        match self {
            Self::Error(err) => Some(err.clone()),
            _ => None,
        }
    }

    /// Create a custom event
    #[must_use]
    pub fn custom<T: 'static + Send + Sync>(data: T) -> Self {
        Self::Custom(Box::new(data))
    }
}
