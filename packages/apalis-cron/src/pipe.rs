use apalis_core::backend::Backend;
use apalis_core::error::BoxDynError;
use apalis_core::request::BoxStream;
use apalis_core::{poller::Poller, request::Request, worker::Context, worker::Worker};
use futures::StreamExt;
use std::{error, fmt};

/// A generic Pipe that wraps an inner type along with a `RequestStream`.
pub struct CronPipe<Inner> {
    pub(crate) stream: BoxStream<'static, Result<(), BoxDynError>>,
    pub(crate) inner: Inner,
}

impl<Inner: fmt::Debug> fmt::Debug for CronPipe<Inner> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pipe")
            .field("stream", &"<RequestStream<()>>") // Placeholder as `RequestStream` might not implement Debug
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T, Ctx, Inner> Backend<Request<T, Ctx>> for CronPipe<Inner>
where
    Inner: Backend<Request<T, Ctx>>,
{
    type Stream = Inner::Stream;

    type Layer = Inner::Layer;

    type Codec = Inner::Codec;

    fn poll(mut self, worker: &Worker<Context>) -> Poller<Self::Stream, Self::Layer> {
        let pipe_heartbeat = async move { while (self.stream.next().await).is_some() {} };
        let inner = self.inner.poll(worker);
        let heartbeat = inner.heartbeat;

        Poller::new_with_layer(
            inner.stream,
            async {
                futures::join!(heartbeat, pipe_heartbeat);
            },
            inner.layer,
        )
    }
}

/// A cron error
#[derive(Debug)]
pub struct PipeError {
    kind: PipeErrorKind,
}

/// The kind of pipe error that occurred
#[derive(Debug)]
pub enum PipeErrorKind {
    /// The cron stream provided a None
    EmptyStream,
}

impl fmt::Display for PipeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            PipeErrorKind::EmptyStream => write!(f, "The cron stream provided a None",),
        }
    }
}

impl error::Error for PipeError {}

impl From<PipeErrorKind> for PipeError {
    fn from(kind: PipeErrorKind) -> PipeError {
        PipeError { kind }
    }
}
