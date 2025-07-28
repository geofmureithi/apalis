use std::time::Duration;

// use futures::{channel::mpsc::Receiver, stream::BoxStream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::workflow::stepped::StepRequest;

// use crate::{backend::Backend, error::BoxDynError, request::Request, worker::context::WorkerContext};

pub mod dag;
pub mod stepped;
// pub mod branch;
// pub mod sink;
// pub mod layer;

/// Allows control of the next flow
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum GoTo<N> {
    /// Go to the next flow immediately
    Next(N),
    /// Delay the next flow for some time
    Delay {
        /// The input of the next flow
        next: N,
        /// The period to delay
        delay: Duration,
    },
    /// Complete execution
    Done(N),
}

pub enum ComplexRequest<Compact> {
    Step(StepRequest<Compact>)
}
