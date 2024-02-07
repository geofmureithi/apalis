//! # Message queue
//!
//! The `mq` module provides an abstraction for representing a message queue that supports pushing and consuming messages. It defines the MessageQueue trait, which can be implemented by different types of message queues.

use futures::Future;

use crate::{request::Request, Backend};

/// Represents a message queue that can be pushed and consumed.
pub trait MessageQueue<Message>: Backend<Request<Message>> {
    /// The error produced by the queue
    type Error;

    /// Enqueues a message to the queue.
    fn enqueue(&self, message: Message) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Attempts to dequeue a message from the queue.
    /// Returns `None` if the queue is empty.
    fn dequeue(&self) -> impl Future<Output = Result<Option<Message>, Self::Error>> + Send;

    /// Returns the current size of the queue.
    fn size(&self) -> impl Future<Output = Result<usize, Self::Error>> + Send;
}

/// Trait representing a job.
///
///
/// # Example
/// ```rust
/// # use apalis_core::mq::Message;
/// # struct Email;
/// impl Message for Email {
///     const NAME: &'static str = "redis::Email";
/// }
/// ```
pub trait Message {
    /// Represents the name for job.
    const NAME: &'static str;
}
