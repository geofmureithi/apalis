use crate::backend::codec::Encoder;
use crate::backend::{BackendWithSink, TaskSink};
use crate::error::BoxDynError;
use crate::request::Request;
use crate::{backend::Backend, worker::context::WorkerContext};
use futures_sink::Sink;
use futures_util::stream::{once, select};
use futures_util::{stream::BoxStream, StreamExt};
use futures_util::{SinkExt, Stream, TryStreamExt};
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;

/// A generic Pipe that wraps an inner type along with a `RequestStream`.
pub struct Pipe<S, Into, Args, Ctx> {
    pub(crate) from: S,
    pub(crate) into: Into,
    pub(crate) _req: PhantomData<(Args, Ctx)>,
}

impl<S, Into, Args, Ctx> Pipe<S, Into, Args, Ctx> {
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

impl<Args, Ctx, S, I, Compact, Err> Backend<Args, Ctx> for Pipe<S, I, Args, Ctx>
where
    S: Stream<Item = Result<Args, Err>> + Send + 'static,
    I: BackendWithSink<Args, Ctx>,
    I::Error: Into<BoxDynError> + Send + Sync + 'static,
    I::Sink: TaskSink<Args, Compact = Compact> + 'static + Sink<Request<Args, Ctx>>,
    I::Beat: Send + 'static,
    <<I as BackendWithSink<Args, Ctx>>::Sink as TaskSink<Args>>::Codec:
        Encoder<Args, Compact = Compact>,

    I::Stream: Send + 'static,
    Args: Send + 'static,
    Ctx: Send + 'static + Default,
    Err: Into<BoxDynError> + Send + Sync + 'static,
    <<I as BackendWithSink<Args, Ctx>>::Sink as Sink<Request<Args, Ctx>>>::Error:
        Into<BoxDynError> + Send + Sync + 'static,
{
    type Stream = BoxStream<'static, Result<Option<Request<Args, Ctx>>, PipeError>>;

    type Layer = I::Layer;

    type Beat = BoxStream<'static, Result<(), PipeError>>;

    type Error = PipeError;

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
        let mut sink = self.into.sink().sink_map_err(|e| e.into());

        let mut sink_stream = self
            .from
            .map_ok(|s| Request::new(s))
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

impl<S, I, Args, Ctx, Compact, Err> BackendWithSink<Args, Ctx>
    for Pipe<S, I, Args, Ctx>
where
    S: Stream<Item = Result<Args, Err>> + Send + 'static,
    I: BackendWithSink<Args, Ctx>,
    I::Error: Into<BoxDynError> + Send + Sync + 'static,
    I::Sink: TaskSink<Args, Compact = Compact> + 'static + Sink<Request<Args, Ctx>>,
    I::Beat: Send + 'static,
    <<I as BackendWithSink<Args, Ctx>>::Sink as TaskSink<Args>>::Codec:
        Encoder<Args, Compact = Compact>,

    I::Stream: Send + 'static,
    Args: Send + 'static,
    Ctx: Send + 'static + Default,
    Err: Into<BoxDynError> + Send + Sync + 'static,
    <<I as BackendWithSink<Args, Ctx>>::Sink as Sink<Request<Args, Ctx>>>::Error:
        Into<BoxDynError> + Send + Sync + 'static,
{
    type Sink = I::Sink;

    fn sink(&self) -> Self::Sink {
        self.into.sink()
    }
}

pub trait PipeExt<B, Args, Ctx>
where
    Self: Sized,
{
    fn pipe_to(self, backend: B) -> Pipe<Self, B, Args, Ctx>;
}

impl<B, Args, Ctx, Err> PipeExt<B, Args, Ctx> for BoxStream<'static, Result<Args, Err>>
where
    B: BackendWithSink<Args, Ctx>,
    B::Error: Into<BoxDynError> + Send + Sync + 'static,
    B::Sink: TaskSink<Args, Context = Ctx>,
{
    fn pipe_to(self, backend: B) -> Pipe<Self, B, Args, Ctx> {
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
    use tower::limit::ConcurrencyLimitLayer;

    use crate::{
        backend::{
            self,
            memory::{MemoryStorage, MemoryWrapper},
            TaskSink,
        },
        error::BoxDynError,
        worker::{
            builder::WorkerBuilder,
            context::WorkerContext,
            ext::{circuit_breaker::CircuitBreaker, event_listener::EventListenerExt},
        },
    };

    use super::*;

    const ITEMS: u32 = 10;

    #[tokio::test]
    async fn basic_worker() {
        let stm = stream::iter(0..ITEMS).map(|s| Ok::<_, io::Error>(s));
        let in_memory = MemoryStorage::new_with_json();

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
