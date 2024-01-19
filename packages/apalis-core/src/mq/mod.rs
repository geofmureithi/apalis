//! # Message queue
//!
//! The `mq` module provides an abstraction for representing a message queue that supports pushing and consuming messages. It defines the MessageQueue trait, which can be implemented by different types of message queues.
use crate::Backend;

/// Represents a message queue that can be pushed and consumed.
#[trait_variant::make(MessageQueue: Send)]
pub trait LocalMessageQueue<Message> {
    /// The error produced by the queue
    type Error;

    /// Enqueues a message to the queue.
    async fn enqueue(&self, message: Message) -> Result<(), Self::Error>;

    /// Attempts to dequeue a message from the queue.
    /// Returns `None` if the queue is empty.
    async fn dequeue(&self) -> Result<Option<Message>, Self::Error>;

    /// Returns the current size of the queue.
    async fn size(&self) -> Result<usize, Self::Error>;
}
