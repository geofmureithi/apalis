//! # Message queue 
//! 
//! Message queueing for apalis via `mq` module and feature respectively
//! The `mq` module provides an abstraction for representing a message queue that supports pushing and consuming messages. It defines the MessageQueue trait, which can be implemented by different types of message queues.
//! Trait: `MessageQueue<J>`

//! The `MessageQueue` trait represents a message queue that can be pushed and consumed. It provides methods for pushing messages and consuming a stream of messages.
//! 
//! ## Trait Methods
//!
//!    - `push(data: J) -> Result<(), JobError>`.

//!    Pushes a new message onto the message queue.

//!    Parameters:
//!        data (J): The message to be pushed onto the queue.

//!    Returns:
//!        Result<(), JobError>: A result indicating the success or failure of the push operation. Returns Ok(()) if the push was successful, or an error of type JobError if an error occurred.

//!    - `consume(worker: &WorkerId) -> JobStreamResult<J>`

//!    Starts consuming a stream of messages from the message queue.

//!    Parameters:
//!        worker (&WorkerId): The identifier of the worker consuming the messages.
//!
//!    Returns:
//!        JobStreamResult<J>: A result representing a stream of messages (J). The stream can be processed asynchronously, allowing the worker to consume messages from the queue.



use crate::{builder::WorkerBuilder, error::JobError, job::JobStreamResult, worker::WorkerId};

/// [WorkerBuilder] utilities for building message queue workers.
pub mod builder;

/// Represents a message queue that can be pushed and consumed.
#[async_trait::async_trait]
pub trait MessageQueue<J> {
    /// Push a new message
    async fn push(&self, data: J) -> Result<(), JobError>;
    /// Start consuming a stream of messages
    fn consume(&self, worker: &WorkerId) -> JobStreamResult<J>;
}

/// A helper trait to help build a [WorkerBuilder] that consumes a [MessageQueue]
pub trait WithMq<NS, Mq: MessageQueue<Self::Job>>: Sized {
    /// The job to consume
    type Job;
    /// The [MessageQueue] to produce jobs
    type Stream;
    /// The builder method to produce a default [WorkerBuilder] that will consume jobs
    fn with_mq(self, mq: Mq) -> WorkerBuilder<Self::Job, Self::Stream, NS>;
}
