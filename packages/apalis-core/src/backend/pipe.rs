//! # Pipe streams to backends
//!
//! This backend allows you to pipe tasks from any stream into another backend.
//! It is useful for connecting different backends together, such as piping tasks
//! from a cron stream into a database backend, or transforming and forwarding tasks
//! between systems.
//!
//! ## Example
//!
//! ```rust
//! # use futures_util::stream;
//! # use apalis_core::backend::pipe::PipeExt;
//! # use apalis_core::backend::json::JsonStorage;
//! # use apalis_core::worker::{builder::WorkerBuilder, context::WorkerContext};
//! # use apalis_core::error::BoxDynError;
//! # use std::time::Duration;
//! # use futures_util::StreamExt;
//! # use crate::apalis_core::worker::ext::event_listener::EventListenerExt;
//! #[tokio::main]
//! async fn main() {
//!     let stm = stream::iter(0..10).map(|s| Ok::<_, std::io::Error>(s));
//!
//!     let in_memory = JsonStorage::new_temp().unwrap();
//!     let backend = stm.pipe_to(in_memory);
//!
//!     async fn task(task: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
//!         tokio::time::sleep(Duration::from_secs(1)).await;
//! #        if task == 9 {
//! #            ctx.stop().unwrap();
//! #        }
//!         Ok(())
//!     }
//!
//!     let worker = WorkerBuilder::new("rango-tango")
//!         .backend(backend)
//!         .on_event(|_ctx, ev| {
//!             println!("On Event = {:?}", ev);
//!         })
//!         .build(task);
//!     worker.run().await.unwrap();
//! }
//! ```
//!
//! This example pipes a stream of numbers into an in-memory backend and processes them with a worker.
//!
//! See also:
//! - [`apalis-cron`](https://docs.rs/apalis-cron)

use crate::backend::TaskSink;
use crate::error::BoxDynError;
use crate::features_table;
use crate::task::Task;
use crate::{backend::Backend, backend::codec::Codec, worker::context::WorkerContext};
use futures_sink::Sink;
use futures_util::stream::{once, select};
use futures_util::{SinkExt, Stream, TryStreamExt};
use futures_util::{StreamExt, stream::BoxStream};
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

/// A generic pipe that wraps a [`Stream`] and passes it to a backend
#[doc = features_table! {
    setup = "unreachable!();";,
    TaskSink => supported("Ability to push new tasks", false),
    InheritsFeatures => limited("Inherits features from the underlying backend", false),
}]
pub struct Pipe<S, Into, Args, Ctx> {
    pub(crate) from: S,
    pub(crate) into: Into,
    pub(crate) _req: PhantomData<(Args, Ctx)>,
}

impl<S: Clone, Into: Clone, Args, Ctx> Clone for Pipe<S, Into, Args, Ctx> {
    fn clone(&self) -> Self {
        Pipe {
            from: self.from.clone(),
            into: self.into.clone(),
            _req: PhantomData,
        }
    }
}

impl<S, Into, Args, Ctx> Deref for Pipe<S, Into, Args, Ctx> {
    type Target = Into;

    fn deref(&self) -> &Self::Target {
        &self.into
    }
}

impl<S, Into, Args, Ctx> DerefMut for Pipe<S, Into, Args, Ctx> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.into
    }
}

impl<S, Into, Args, Ctx> Pipe<S, Into, Args, Ctx> {
    /// Create a new Pipe instance
    pub fn new(stream: S, backend: Into) -> Self {
        Pipe {
            from: stream,
            into: backend,
            _req: PhantomData,
        }
    }
}

impl<S: fmt::Debug, Into: fmt::Debug, Args, Ctx> fmt::Debug for Pipe<S, Into, Args, Ctx> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pipe")
            .field("inner", &self.from)
            .field("into", &self.into)
            .finish()
    }
}

impl<Args, Ctx, S, TSink, Err> Backend for Pipe<S, TSink, Args, Ctx>
where
    S: Stream<Item = Result<Args, Err>> + Send + 'static,
    TSink: Backend<Args = Args, Context = Ctx>
        + TaskSink<Args>
        + Clone
        + Unpin
        + Send
        + 'static
        + Sink<Task<TSink::Compact, Ctx, TSink::IdType>>,
    <TSink as Backend>::Error: std::error::Error + Send + Sync + 'static,
    TSink::Beat: Send + 'static,
    TSink::IdType: Send + Clone + 'static,
    TSink::Stream: Send + 'static,
    Args: Send + 'static,
    Ctx: Send + 'static + Default,
    Err: std::error::Error + Send + Sync + 'static,
    <TSink as Sink<Task<TSink::Compact, Ctx, TSink::IdType>>>::Error:
        std::error::Error + Send + Sync + 'static,
    <<TSink as Backend>::Codec as Codec<Args>>::Error: std::error::Error + Send + Sync + 'static,
    TSink::Compact: Send,
{
    type Args = Args;

    type IdType = TSink::IdType;

    type Context = Ctx;

    type Stream = BoxStream<'static, Result<Option<Task<Args, Ctx, Self::IdType>>, PipeError>>;

    type Layer = TSink::Layer;

    type Beat = BoxStream<'static, Result<(), PipeError>>;

    type Error = PipeError;

    type Codec = TSink::Codec;

    type Compact = TSink::Compact;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        self.into
            .heartbeat(worker)
            .map_err(|e| PipeError::Inner(e.into()))
            .boxed()
    }

    fn middleware(&self) -> Self::Layer {
        self.into.middleware()
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        let mut sink = self
            .into
            .clone()
            .sink_map_err(|e| PipeError::Inner(e.into()));

        let mut sink_stream = self
            .from
            .map_err(|e| PipeError::Inner(e.into()))
            .map_ok(|s| {
                Task::new(s)
                    .try_map(|s| TSink::Codec::encode(&s).map_err(|e| PipeError::Inner(e.into())))
            })
            .map(|t| t.and_then(|t| t))
            .boxed();

        let sender_stream = self.into.poll(worker);
        select(
            once(async move {
                let fut = sink.send_all(&mut sink_stream);
                fut.await.map_err(|e| PipeError::Inner(e.into()))?;
                Ok(None)
            }),
            sender_stream.map_err(|e| PipeError::Inner(e.into())),
        )
        .boxed()
    }
}

/// Represents utility for piping streams into a backend
pub trait PipeExt<B, Args, Ctx>
where
    Self: Sized,
{
    /// Pipe the current stream into the provided backend
    fn pipe_to(self, backend: B) -> Pipe<Self, B, Args, Ctx>;
}

impl<B, Args, Ctx, Err, S> PipeExt<B, Args, Ctx> for S
where
    S: Stream<Item = Result<Args, Err>> + Send + 'static,
    <B as Backend>::Error: Into<BoxDynError> + Send + Sync + 'static,
    B: Backend<Args = Args> + TaskSink<Args>,
{
    fn pipe_to(self, backend: B) -> Pipe<Self, B, Args, Ctx> {
        Pipe::new(self, backend)
    }
}

/// Error encountered while piping streams
#[derive(Debug, thiserror::Error)]
pub enum PipeError {
    /// The cron stream provided a None
    #[error("The inner stream provided a None")]
    EmptyStream,
    /// An inner stream error occurred
    #[error("The inner stream error: {0}")]
    Inner(BoxDynError),
}

#[cfg(test)]
#[cfg(feature = "json")]
mod tests {
    use std::{io, time::Duration};

    use futures_util::stream;

    use crate::{
        backend::json::JsonStorage,
        error::BoxDynError,
        worker::{
            builder::WorkerBuilder, context::WorkerContext, ext::event_listener::EventListenerExt,
        },
    };

    use super::*;

    const ITEMS: u32 = 10;

    #[tokio::test]
    async fn basic_worker() {
        let stm = stream::iter(0..ITEMS).map(|s| Ok::<_, io::Error>(s));
        let in_memory = JsonStorage::new_temp().unwrap();

        let backend = Pipe::new(stm, in_memory);

        async fn task(task: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task == ITEMS - 1 {
                ctx.stop().unwrap();
                return Err("Graceful Exit".into());
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|_ctx, ev| {
                println!("On Event = {:?}", ev);
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
