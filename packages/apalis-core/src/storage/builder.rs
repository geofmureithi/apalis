use futures::StreamExt;
use std::{marker::PhantomData, time::Duration};
use tower::layer::util::Stack;

use crate::{builder::WorkerBuilder, job::JobStreamResult, worker::WorkerRef};

use super::{layers::KeepAliveLayer, Storage};

/// A helper trait to help build a [Worker] that consumes a [Storage]
pub trait WithStorage<NS, ST: Storage<Output = Self::Job>> {
    /// The job to consume
    type Job;
    /// The [Stream] to produce jobs
    type Stream;
    /// The builder method to produce a [WorkerBuilder] that will consume jobs
    fn with_storage(self, storage: ST) -> WorkerBuilder<Self::Job, Self::Stream, NS>;
}

impl<J: 'static, M, ST> WithStorage<Stack<KeepAliveLayer<ST, J>, M>, ST>
    for WorkerBuilder<(), (), M>
where
    ST: Storage<Output = J>,
{
    type Job = J;
    type Stream = JobStreamResult<J>;
    fn with_storage(
        self,
        storage: ST,
    ) -> WorkerBuilder<J, Self::Stream, Stack<KeepAliveLayer<ST, J>, M>> {
        let layer = self.layer.layer(KeepAliveLayer::new(
            WorkerRef::new(self.name.clone()),
            storage.clone(),
            Duration::from_secs(30),
        ));
        let mut storage = storage;
        WorkerBuilder {
            job: PhantomData,
            layer,
            source: storage
                .consume(self.name.clone(), Duration::from_millis(10))
                .boxed(),
            name: self.name,
        }
    }
}
