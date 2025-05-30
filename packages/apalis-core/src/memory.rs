use crate::{
    backend::{Backend, Push, Schedule}, error::BoxDynError, request::{Parts, Request, RequestStream}, worker::{self, WorkerContext}
};
use futures::{
    channel::mpsc::{channel, Receiver, Sender},
    stream::{self, BoxStream},
    SinkExt, Stream, StreamExt,
};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tower::layer::util::Identity;

#[derive(Debug)]
/// An example of the basics of a backend
pub struct MemoryStorage<T> {
    /// This would be the backend you are targeting, eg a connection poll
    inner: MemoryWrapper<T>,
}
impl<T> MemoryStorage<T> {
    /// Create a new in-memory storage
    pub fn new() -> Self {
        Self {
            inner: MemoryWrapper::new(),
        }
    }
}

impl<T> Default for MemoryStorage<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for MemoryStorage<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// In-memory queue that implements [Stream]
#[derive(Debug)]
pub struct MemoryWrapper<T> {
    sender: Sender<Request<T, ()>>,
    receiver: Arc<futures::lock::Mutex<Receiver<Request<T, ()>>>>,
}

impl<T> Clone for MemoryWrapper<T> {
    fn clone(&self) -> Self {
        Self {
            receiver: self.receiver.clone(),
            sender: self.sender.clone(),
        }
    }
}

impl<T> MemoryWrapper<T> {
    /// Build a new basic queue channel
    pub fn new() -> Self {
        let (sender, receiver) = channel(100);

        Self {
            sender,
            receiver: Arc::new(futures::lock::Mutex::new(receiver)),
        }
    }
}

impl<T> Default for MemoryWrapper<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Stream for MemoryWrapper<T> {
    type Item = Request<T, ()>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(mut receiver) = self.receiver.try_lock() {
            receiver.poll_next_unpin(cx)
        } else {
            Poll::Pending
        }
    }
}

// MemoryStorage as a Backend
impl<T: Send + 'static + Sync> Backend<Request<T, ()>> for MemoryStorage<T> {
    type Error = BoxDynError;
    type Stream = RequestStream<Request<T, ()>>;
    type Layer = Identity;
    type Beat = BoxStream<'static, Result<(), Self::Error>>;
    type Codec = ();
    fn heartbeat(&self) -> Self::Beat {
        stream::once(async { Ok(()) }).boxed()
    }
    fn middleware(&self) -> Self::Layer {
        Identity::new()
    }

    fn poll(self, _worker: &WorkerContext) -> Self::Stream {
        let stream = self.inner.map(|r| Ok(Some(r))).boxed();
        stream
    }
}

impl<Req: Send + Sync + 'static> Push<Req, ()> for MemoryStorage<Req> {
    type Compact = Req;
    async fn push_request(&mut self, req: Request<Req, ()>) -> Result<Parts<()>, Self::Error> {
        self.push_raw_request(req).await
    }

    async fn push_raw_request(
        &mut self,
        req: Request<Self::Compact, ()>,
    ) -> Result<Parts<()>, Self::Error> {
        let parts = req.parts.clone();
        self.inner.sender.send(req).await.unwrap();
        Ok(parts)
    }
}

impl<Req: Send + Sync + 'static> Schedule<Req, ()> for MemoryStorage<Req> {
    type Timestamp = Duration;
    async fn schedule_request(
        &mut self,
        req: Request<Req, ()>,
        on: Self::Timestamp,
    ) -> Result<Parts<()>, Self::Error> {
        // sleep(on).await;
        let parts = req.parts.clone();
        self.inner.sender.send(req).await.unwrap();
        Ok(parts)
    }

    async fn schedule_raw_request(
        &mut self,
        req: Request<Self::Compact, ()>,
        on: Self::Timestamp,
    ) -> Result<Parts<()>, Self::Error> {
        unreachable!("Requests must be typed")
    }
}
