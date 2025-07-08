use std::time::Duration;

// use futures::{channel::mpsc::Receiver, stream::BoxStream, StreamExt};
use serde::{Deserialize, Serialize};

// use crate::{backend::Backend, error::BoxDynError, request::Request, worker::context::WorkerContext};

pub mod dag;
pub mod stepped;
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

// pub struct PollWithPushBackend<B, T, Ctx> {
//     backend: B,
//     receiver: Receiver<Request<T, Ctx>>,
// }



// impl<T, Ctx, B> Backend<Request<T, Ctx>> for PollWithPushBackend<B, T, Ctx>
// where
//     B: Backend<Request<T, Ctx>, Error = BoxDynError>,
// {
//     type Error = BoxDynError;
//     type Stream = Self;
//     type Layer = B::Layer;
//     type Beat = BoxStream<'static, Result<(), BoxDynError>>;
//     fn heartbeat(&self) -> Self::Beat {
//         self.backend.heartbeat().boxed()
//     }
//     fn middleware(&self) -> Self::Layer {
//         self.backend.middleware()
//     }
//     fn poll(self, _: &WorkerContext) -> Self::Stream {
//         self.backend
//     }
// }
