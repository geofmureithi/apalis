use std::marker::PhantomData;

use futures::StreamExt;
use tower::layer::util::Stack;

use crate::{
    builder::WorkerBuilder,
    job::JobStreamResult,
    layers::{
        ack::{Ack, AckLayer},
        extensions::Extension,
    },
};

use super::{MessageQueue, WithMq};

impl<J: 'static, M, Mq> WithMq<Stack<Extension<Mq>, Stack<AckLayer<Mq, J>, M>>, Mq>
    for WorkerBuilder<(), (), M>
where
    Mq: MessageQueue<J> + Send + Sync + 'static + Clone + Ack<J>,
    M: Send + Sync + 'static,
{
    type Job = J;
    type Stream = JobStreamResult<J>;
    fn with_mq(
        self,
        mq: Mq,
    ) -> WorkerBuilder<J, Self::Stream, Stack<Extension<Mq>, Stack<AckLayer<Mq, J>, M>>> {
        let worker_id = self.id;
        let source = mq.consume(&worker_id).boxed();

        let layer = self
            .layer
            .layer(AckLayer::new(mq.clone(), worker_id.clone()))
            .layer(Extension(mq.clone()));

        WorkerBuilder {
            job: PhantomData,
            layer,
            source,
            id: worker_id,
            beats: self.beats,
        }
    }
}
