use crate::{
    mq::MessageQueue,
    poller::{controller::Controller, stream::BackendStream},
    request::{Request, RequestStream},
    worker::WorkerId,
    Backend, Poller,
};
use futures::{
    channel::mpsc::{channel, Receiver, Sender},
    Stream, StreamExt,
};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tower::{layer::util::Identity, ServiceBuilder};

#[derive(Debug)]
/// An example of the basics of a backend
pub struct MemoryStorage<T> {
    /// Required for [Poller] to control polling.
    controller: Controller,
    /// This would be the backend you are targeting, eg a connection poll
    inner: MemoryWrapper<T>,
}
impl<T> MemoryStorage<T> {
    /// Create a new in-memory storage
    pub fn new() -> Self {
        Self {
            controller: Controller::new(),
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
            controller: self.controller.clone(),
            inner: self.inner.clone(),
        }
    }
}

/// In-memory queue that implements [Stream]
#[derive(Debug)]
pub struct MemoryWrapper<T> {
    sender: Sender<T>,
    receiver: Arc<futures::lock::Mutex<Receiver<T>>>,
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
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(mut receiver) = self.receiver.try_lock() {
            receiver.poll_next_unpin(cx)
        } else {
            Poll::Pending
        }
    }
}

// MemoryStorage as a Backend
impl<T: Send + 'static + Sync> Backend<Request<T>> for MemoryStorage<T> {
    type Stream = BackendStream<RequestStream<Request<T>>>;

    type Layer = ServiceBuilder<Identity>;

    fn common_layer(&self, _worker: WorkerId) -> Self::Layer {
        ServiceBuilder::new()
    }

    fn poll(self, _worker: WorkerId) -> Poller<Self::Stream> {
        let stream = self.inner.map(|r| Ok(Some(Request::new(r)))).boxed();
        Poller {
            stream: BackendStream::new(stream, self.controller),
            heartbeat: Box::pin(async {}),
        }
    }
}

impl<Message: Send + 'static + Sync> MessageQueue<Message> for MemoryStorage<Message> {
    type Error = ();
    async fn enqueue(&self, message: Message) -> Result<(), Self::Error> {
        self.inner.sender.clone().try_send(message).unwrap();
        Ok(())
    }

    async fn dequeue(&self) -> Result<Option<Message>, ()> {
        Err(())
        // self.inner.receiver.lock().await.next().await
    }

    async fn size(&self) -> Result<usize, ()> {
        Ok(self.inner.clone().count().await)
    }
}
