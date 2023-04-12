use crate::{error::JobError, job::JobStreamResult, worker::WorkerId, builder::WorkerBuilder};

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