use super::{
    future::ResponseFuture,
    message::Message,
    worker::{Handle, Worker},
};

use futures::channel::{mpsc, oneshot};
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use futures::task::AtomicWaker;
use std::sync::Arc;
use std::{
    future::Future,
    task::{Context, Poll},
};
use tower::Service;

/// Adds an mpsc buffer in front of an inner service.
///
/// See the module documentation for more details.
#[derive(Debug)]
pub struct Buffer<Req, F> {
    tx: PollSender<Message<Req, F>>,
    handle: Handle,
}

impl<Req, F> Buffer<Req, F>
where
    F: 'static,
{
    /// Creates a new [`Buffer`] wrapping `service`, but returns the background worker.
    ///
    /// This is useful if you do not want to spawn directly onto the runtime
    /// but instead want to use your own executor. This will return the [`Buffer`] and
    /// the background `Worker` that you can then spawn.
    pub fn pair<S>(service: S, bound: usize) -> (Self, Worker<S, Req>)
    where
        S: Service<Req, Future = F> + Send + 'static,
        F: Send,
        S::Error: Into<tower::BoxError> + Send + Sync,
        Req: Send + 'static,
    {
        let (tx, rx) = mpsc::channel(bound);
        let (handle, worker) = Worker::new(service, rx);
        let buffer = Self {
            tx: PollSender::new(tx),
            handle,
        };
        (buffer, worker)
    }

    fn get_worker_error(&self) -> tower::BoxError {
        self.handle.get_error_on_closed()
    }
}

impl<Req, Rsp, F, E> Service<Req> for Buffer<Req, F>
where
    F: Future<Output = Result<Rsp, E>> + Send + 'static,
    E: Into<tower::BoxError>,
    Req: Send + 'static,
{
    type Response = Rsp;
    type Error = tower::BoxError;
    type Future = ResponseFuture<F>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // First, check if the worker is still alive.
        if self.tx.is_closed() {
            // If the inner service has errored, then we error here.
            return Poll::Ready(Err(self.get_worker_error()));
        }

        // Poll the sender to acquire a permit.
        self.tx
            .poll_reserve(cx)
            .map_err(|_| self.get_worker_error())
    }

    fn call(&mut self, request: Req) -> Self::Future {
        // tracing::trace!("sending request to buffer worker");

        // get the current Span so that we can explicitly propagate it to the worker
        // if we didn't do this, events on the worker related to this span wouldn't be counted
        // towards that span since the worker would have no way of entering it.
        // let span = tracing::Span::current();

        // If we've made it here, then a channel permit has already been
        // acquired, so we can freely allocate a oneshot.
        let (tx, rx) = oneshot::channel();

        match self.tx.send_item(Message { request, tx }) {
            Ok(_) => ResponseFuture::new(rx),
            // If the channel is closed, propagate the error from the worker.
            Err(_) => {
                // tracing::trace!("buffer channel closed");
                ResponseFuture::failed(self.get_worker_error())
            }
        }
    }
}

impl<Req, F> Clone for Buffer<Req, F>
where
    Req: Send + 'static,
    F: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            tx: self.tx.clone(),
        }
    }
}

// PollSender implementation using futures and async-channel
#[derive(Debug)]
struct PollSender<T> {
    tx: mpsc::Sender<T>,
    waker: Arc<AtomicWaker>,
}

impl<T> PollSender<T> {
    fn new(tx: mpsc::Sender<T>) -> Self {
        Self {
            tx,
            waker: Arc::new(AtomicWaker::new()),
        }
    }

    fn poll_reserve(&self, cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
        if self.tx.is_closed() {
            return Poll::Ready(Err(()));
        }

        self.waker.register(cx.waker());

        Poll::Ready(Ok(()))
    }

    fn send_item(&mut self, item: T) -> Result<(), ()> {
        if self.tx.is_closed() {
            return Err(());
        }

        self.tx.try_send(item).map_err(|_| ())
    }

    fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            waker: self.waker.clone(),
        }
    }
}
