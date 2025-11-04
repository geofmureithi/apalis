use std::{convert::Infallible, fmt::Debug, marker::PhantomData, time::Duration};

use apalis_core::{
    backend::{Backend, WeakTaskSink, codec::Codec},
    error::BoxDynError,
    task::{Task, metadata::MetadataExt},
};
use futures::Sink;

use crate::{GenerateId, GoTo, Step, Workflow, WorkflowRequest, context::StepContext};

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
    FlowSink: Sync + Unpin + WeakTaskSink<Current> + Send,
    Current: Send,
    FlowSink::Context: Send + Sync + Default + MetadataExt<WorkflowRequest>,
    FlowSink::Error: Into<BoxDynError> + Send + 'static,
    FlowSink::IdType: GenerateId + Send,
    Compact: Sync + Send,
    Encode: Codec<Current, Compact = Compact> + Sync + Send + 'static,
    Encode::Error: std::error::Error + Sync + Send + 'static,
    <FlowSink::Context as MetadataExt<WorkflowRequest>>::Error:
        std::error::Error + Sync + Send + 'static,
{
    type Response = Current;
    type Error = Infallible;

    async fn run(
        &mut self,
        _: &StepContext<FlowSink, Encode>,
        task: Task<Current, FlowSink::Context, FlowSink::IdType>,
    ) -> Result<GoTo<Current>, Self::Error> {
        Ok(GoTo::DelayFor(self.duration, task.args))
    }
}

impl<Input, Current, FlowSink, Encode, Compact>
    Workflow<Input, Current, FlowSink, Encode, Compact, FlowSink::Context, FlowSink::IdType>
where
    Current: Send + 'static,
    FlowSink: Send + Clone + Sync + 'static + Unpin + WeakTaskSink<Current, Codec = Encode>,
{
    pub fn delay_for<CodecError, DbError>(
        self,
        duration: Duration,
    ) -> Workflow<Input, Current, FlowSink, Encode, Compact, FlowSink::Context, FlowSink::IdType>
    where
        Current: std::marker::Send + 'static + Sync,
        FlowSink::Context: Send + Sync + Default + 'static + MetadataExt<WorkflowRequest>,
        FlowSink: Sink<Task<Compact, FlowSink::Context, FlowSink::IdType>, Error = DbError>
            + Backend<Error = DbError>,
        DbError: std::error::Error + Send + Sync + 'static,
        FlowSink::IdType: Send + GenerateId,
        Compact: Sync + Send + 'static,
        Encode: Codec<Current, Compact = Compact, Error = CodecError> + Send + Sync + 'static,
        Encode: Codec<GoTo<Current>, Compact = Compact, Error = CodecError> + Send + Sync + 'static,
        CodecError: Send + Sync + std::error::Error + 'static,
        <FlowSink::Context as MetadataExt<WorkflowRequest>>::Error:
            std::error::Error + Sync + Send + 'static,
    {
        self.add_step::<_, Current, _, _, DbError>(DelayStep {
            duration,
            _marker: PhantomData,
        })
    }
}
