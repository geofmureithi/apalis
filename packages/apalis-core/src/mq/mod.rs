//! # Message queue
//!
//! The `mq` module provides an abstraction for representing a message queue that supports pushing and consuming messages. It defines the MessageQueue trait, which can be implemented by different types of message queues.

use futures::{Future, FutureExt};

use crate::{backend::Backend, request::Request};

/// Represents a message queue that can be pushed and consumed.
pub trait MessageQueue<Message: Send>: Backend<Request<Message, Self::Context>> {
    /// This stores more data about the Message,
    /// provided by the backend
    type Context: Default;
    /// The error produced by the queue
    type Error;

    /// The format that the storage persists the jobs usually `Vec<u8>`
    type Compact;

    /// Enqueues a message to the queue.
    fn enqueue(
        &mut self,
        message: Message,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.enqueue_request(Request::new(message))
    }

    /// Enqueues a Request constructed with customizations
    fn enqueue_request(
        &mut self,
        req: Request<Message, Self::Context>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Enqueues a request without forcing the type
    fn enqueue_raw_request(
        &mut self,
        req: Request<Self::Compact, Self::Context>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Attempts to dequeue a message from the queue.
    /// Returns `None` if the queue is empty.
    fn dequeue(&mut self) -> impl Future<Output = Result<Option<Message>, Self::Error>> + Send {
        self.dequeue_request()
            .map(|req| req.map(|r| r.map(|r| r.args)))
    }

    /// Attempts to dequeue a message from the queue.
    /// Returns `None` if the queue is empty.
    fn dequeue_request(
        &mut self,
    ) -> impl Future<Output = Result<Option<Request<Message, Self::Context>>, Self::Error>> + Send;

    /// Returns the current size of the queue.
    fn size(&mut self) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}
