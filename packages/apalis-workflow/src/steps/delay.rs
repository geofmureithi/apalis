use std::{f32::consts::E, fmt::Debug, marker::PhantomData, time::Duration};

use apalis_core::{
    backend::{codec::Codec, TaskSink},
    error::{BoxDynError, DeferredError, RetryAfterError},
    task::{self, builder::TaskBuilder, metadata::MetadataExt, Task},
    task_fn::{task_fn, TaskFn},
};
use tower::Service;

use crate::{context::StepContext, Step, WorkFlow, WorkflowError, WorkflowRequest};

#[derive(Debug)]
pub struct DelayStep<S, T> {
    duration: S,
    _marker: std::marker::PhantomData<T>,
}

impl<S: Clone, T> Clone for DelayStep<S, T> {
    fn clone(&self) -> Self {
        DelayStep {
            duration: self.duration.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, T> DelayStep<S, T> {
    pub fn new(inner: S) -> Self {
        Self {
            duration: inner,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Current, FlowSink, Encode, Compact> Step<Current, FlowSink, Encode>
    for DelayStep<Duration, Current>
where
    Current: Sync + Send + 'static,
    FlowSink: Sync + Unpin + TaskSink<Compact> + Send,
    Current: Send,
    FlowSink::Context: Send + Sync + Default + MetadataExt<WorkflowRequest>,
    FlowSink::Error: Into<BoxDynError> + Send + 'static,
    FlowSink::IdType: Default + Send,
    Compact: Sync + Send,
    Encode: Codec<Current, Compact = Compact> + Sync + Send + 'static,
    Encode::Error: std::error::Error + Sync + Send + 'static,
    <FlowSink::Context as MetadataExt<WorkflowRequest>>::Error:
        std::error::Error + Sync + Send + 'static,
{
    type Response = Current;
    type Error = RetryAfterError;
    async fn pre(
        ctx: &mut StepContext<FlowSink, Encode>,
        step: &Current,
    ) -> Result<(), Self::Error> {
        ctx.push_next_step(step).await?;
        Ok(())
    }

    async fn run(
        &mut self,
        ctx: &StepContext<FlowSink, Encode>,
        task: Task<Current, FlowSink::Context, FlowSink::IdType>,
    ) -> Result<Self::Response, Self::Error> {
        apalis_core::timer::sleep(self.duration).await;

        Ok(task.args)
    }
}

impl<Input, Current, FlowSink, Encode, Compact> WorkFlow<Input, Current, FlowSink, Encode, Compact>
where
    Current: Send + 'static,
    FlowSink: Send + Clone + Sync + 'static + Unpin + TaskSink<Compact>,
{
    pub fn delay_for<CodecError>(
        self,
        duration: Duration,
    ) -> WorkFlow<Input, Current, FlowSink, Encode, Compact>
    where
        Current: std::marker::Send + 'static + Sync,
        FlowSink::Context: Send + Sync + Default + 'static + MetadataExt<WorkflowRequest>,
        FlowSink::Error: Into<BoxDynError> + Send + 'static,
        FlowSink::IdType: Send + Default,
        Compact: Sync + Send + 'static,
        Encode: Codec<Current, Compact = Compact, Error = CodecError> + Send + Sync + 'static,
        CodecError: Send + Sync + std::error::Error + 'static,
        <FlowSink::Context as MetadataExt<WorkflowRequest>>::Error:
            std::error::Error + Sync + Send + 'static,
    {
        self.add_step::<_, Current, _, _>(DelayStep {
            duration,
            _marker: PhantomData,
        })
    }
}
