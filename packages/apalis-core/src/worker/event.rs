use std::{
    any::Any,
    fmt,
    sync::{Arc, RwLock},
};

use crate::{error::BoxDynError, request::task_id::TaskId, worker::context::WorkerContext};

/// An event handler for [`Worker`]
pub type EventHandler = Arc<RwLock<Option<Box<dyn Fn(&WorkerContext, &Event) + Send + Sync>>>>;

pub type CtxEventHandler = Arc<Box<dyn Fn(&WorkerContext, &Event) + Send + Sync>>;

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
            Event::Start => "Worker started".to_string(),
            Event::Idle => "Worker is idle".to_string(),
            Event::Custom(_) => format!("Custom event"),
            Event::Error(err) => format!("Worker encountered an error: {}", err),
            Event::Stop => "Worker stopped".to_string(),
            Event::HeartBeat => "Worker Heartbeat".to_owned(),
            Event::Success => "Worker completed task successfully".to_string(),
        };

        write!(f, "WorkerEvent: {}", event_description)
    }
}
