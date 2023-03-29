use futures::StreamExt;
use std::{marker::PhantomData, time::Duration};
use tower::layer::util::Stack;

use crate::{builder::WorkerBuilder, job::JobStreamResult};

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
        mut storage: ST,
    ) -> WorkerBuilder<J, Self::Stream, Stack<KeepAliveLayer<ST, J>, M>> {
        let worker_id = self.id;
        let source = storage
            .consume(&worker_id, Duration::from_millis(10))
            .boxed();
        let layer = self.layer.layer(KeepAliveLayer::new(
            worker_id.clone(),
            storage.clone(),
            Duration::from_secs(30),
        ));
        WorkerBuilder {
            job: PhantomData,
            layer,
            source,
            id: worker_id,
        }
    }
}
