//! # Utilities to pipe streams to backends
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
//! # use apalis_core::backend::pipe::Pipe;
//! # use apalis_core::backend::memory::MemoryStorage;
//! # use apalis_core::worker::{WorkerBuilder, context::WorkerContext};
//! # use apalis_core::error::BoxDynError;
//! # use std::time::Duration;
//! #[tokio::main]
//! async fn main() {
//!     let stm = stream::iter(0..10).map(|s| Ok::<_, std::io::Error>(s));
//!
//!     let in_memory = MemoryStorage::new();
//!     let backend = Pipe::new(stm, in_memory);
//!
//!     async fn task(task: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
//!         tokio::time::sleep(Duration::from_secs(1)).await;
//! #        if task == 9 {
//! #            ctx.stop().unwrap();
//! #            return Err("Graceful Exit".into());
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

use crate::error::BoxDynError;
use crate::task::Task;
use crate::{backend::Backend, worker::context::WorkerContext};
use futures_sink::Sink;
use futures_util::stream::{once, select};
use futures_util::{stream::BoxStream, StreamExt};
use futures_util::{SinkExt, Stream, TryStreamExt};
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;

/// A generic Pipe that wraps an inner type along with a `RequestStream`.
pub struct Pipe<S, Into, Args, Meta> {
    pub(crate) from: S,
    pub(crate) into: Into,
    pub(crate) _req: PhantomData<(Args, Meta)>,
}

impl<S, Into, Args, Meta> Pipe<S, Into, Args, Meta> {
    /// Create a new Pipe instance
    pub fn new(stream: S, backend: Into) -> Self {
        Pipe {
            from: stream,
            into: backend,
            _req: PhantomData,
        }
    }
}

impl<S: fmt::Debug, Into: fmt::Debug, Args, Meta> fmt::Debug for Pipe<S, Into, Args, Meta> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pipe")
            .field("inner", &self.from)
            .field("into", &self.into)
            .finish()
    }
}

impl<Args, Meta, S, TSink, Err> Backend<Args> for Pipe<S, TSink, Args, Meta>
where
    S: Stream<Item = Result<Args, Err>> + Send + 'static,
    TSink: Backend<Args, Meta = Meta>
        + Sink<Task<Args, Meta, TSink::IdType>>
        + Clone // TODO: Remove clone
        + Unpin
        + Send
        + 'static,
    <TSink as Backend<Args>>::Error: Into<BoxDynError> + Send + Sync + 'static,
    TSink::Beat: Send + 'static,
    TSink::IdType: Send + Clone + 'static,
    TSink::Stream: Send + 'static,
    Args: Send + 'static,
    Meta: Send + 'static + Default,
    Err: Into<BoxDynError> + Send + Sync + 'static,
    <TSink as Sink<Task<Args, Meta, TSink::IdType>>>::Error:
        Into<BoxDynError> + Send + Sync + 'static,
{
    type IdType = TSink::IdType;

    type Meta = Meta;

    type Stream = BoxStream<'static, Result<Option<Task<Args, Meta, Self::IdType>>, PipeError>>;

    type Layer = TSink::Layer;

    type Beat = BoxStream<'static, Result<(), PipeError>>;

    type Error = PipeError;

    type Codec = TSink::Codec;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        self.into
            .heartbeat(worker)
            .map_err(|e| PipeError {
                kind: PipeErrorKind::Inner(e.into()),
            })
            .boxed()
    }

    fn middleware(&self) -> Self::Layer {
        self.into.middleware()
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        let mut sink = self.into.clone().sink_map_err(|e| e.into());

        let mut sink_stream = self
            .from
            .map_ok(|s| Task::new(s))
            .map_err(|e| e.into())
            .boxed();

        let sender_stream = self.into.poll(worker);
        select(
            once(async move {
                let fut = sink.send_all(&mut sink_stream);
                fut.await.map_err(|e| PipeError {
                    kind: PipeErrorKind::Inner(e.into()),
                })?;
                Ok(None)
            }),
            sender_stream.map_err(|e| PipeError {
                kind: PipeErrorKind::Inner(e.into()),
            }),
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

impl<B, Args, Meta, Err> PipeExt<B, Args, Meta> for BoxStream<'static, Result<Args, Err>>
where
    <B as Backend<Args>>::Error: Into<BoxDynError> + Send + Sync + 'static,
    B: Backend<Args> + Sink<Task<Args, Meta, B::IdType>>,
{
    fn pipe_to(self, backend: B) -> Pipe<Self, B, Args, Meta> {
        Pipe::new(self, backend)
    }
}

/// A pipe error
#[derive(Debug, thiserror::Error)]
pub struct PipeError {
    kind: PipeErrorKind,
}

/// The kind of pipe error that occurred
#[derive(Debug)]
pub enum PipeErrorKind {
    /// The cron stream provided a None
    EmptyStream,
    /// An inner stream error occurred
    Inner(BoxDynError),
}

impl fmt::Display for PipeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            PipeErrorKind::EmptyStream => write!(f, "The inner stream provided a None",),
            PipeErrorKind::Inner(e) => write!(f, "The inner stream error {}", e),
        }
    }
}

impl From<PipeErrorKind> for PipeError {
    fn from(kind: PipeErrorKind) -> PipeError {
        PipeError { kind }
    }
}

#[cfg(test)]
mod tests {
    use std::{io, time::Duration};

    use futures_util::stream;
    

    use crate::{
        backend::json::JsonStorage,
        error::BoxDynError,
        worker::{
            builder::WorkerBuilder,
            context::WorkerContext,
            ext::event_listener::EventListenerExt,
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
