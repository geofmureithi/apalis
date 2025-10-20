use std::{fmt::Debug, marker::PhantomData};

use apalis_core::{
    backend::{Backend, WeakTaskSink, codec::Codec},
    error::BoxDynError,
    task::{Task, metadata::MetadataExt},
    task_fn::{TaskFn, task_fn},
};
use tower::Service;

use crate::{context::StepContext, GenerateId, GoTo, Step, WorkFlow, WorkflowRequest};

#[derive(Debug)]
pub struct ThenStep<S, T> {
    inner: S,
    _marker: std::marker::PhantomData<T>,
}

impl<S: Clone, T> Clone for ThenStep<S, T> {
    fn clone(&self) -> Self {
        ThenStep {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, T> ThenStep<S, T> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S, Current, O, E, FlowSink, Encode, Compact> Step<Current, FlowSink, Encode>
    for ThenStep<S, Current>
where
    S: Service<Task<Current, FlowSink::Context, FlowSink::IdType>, Response = O, Error = E>
        + Sync
        + Send,
    Current: Sync + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    E: Into<BoxDynError> + Send + Sync + 'static,
    O: Sync,
    FlowSink: Sync + Unpin + WeakTaskSink<O> + Send,
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
    type Response = S::Response;
    type Error = S::Error;

    async fn run(
        &mut self,
        _: &StepContext<FlowSink, Encode>,
        args: Task<Current, FlowSink::Context, FlowSink::IdType>,
    ) -> Result<GoTo<S::Response>, Self::Error> {
        let res = self.inner.call(args).await?;
        Ok(GoTo::Next(res))
    }
}

impl<Input, Current, FlowSink, Encode, Compact>
    WorkFlow<Input, Current, FlowSink, Encode, Compact, FlowSink::Context, FlowSink::IdType>
where
    Current: Send + 'static,
    FlowSink: Send + Clone + Sync + 'static + Unpin + Backend,
{
    pub fn then<F, O, E, FnArgs, CodecError>(
        self,
        then: F,
    ) -> WorkFlow<Input, O, FlowSink, Encode, Compact, FlowSink::Context, FlowSink::IdType>
    where
        O: Sync + Send + 'static,
        E: Into<BoxDynError> + Send + Sync + 'static,
        F: Send + 'static + Sync + Clone,
        TaskFn<F, Current, FlowSink::Context, FnArgs>:
            Service<Task<Current, FlowSink::Context, FlowSink::IdType>, Response = O, Error = E>,
        FnArgs: std::marker::Send + 'static + Sync,
        Current: std::marker::Send + 'static + Sync,
        FlowSink::Context: Send + Sync + Default + 'static + MetadataExt<WorkflowRequest>,
        FlowSink::Error: Into<BoxDynError> + Send + 'static,
        <TaskFn<F, Current, FlowSink::Context, FnArgs> as Service<
            Task<Current, FlowSink::Context, FlowSink::IdType>,
        >>::Future: Send + 'static,
        <TaskFn<F, Current, FlowSink::Context, FnArgs> as Service<
            Task<Current, FlowSink::Context, FlowSink::IdType>,
        >>::Error: Into<BoxDynError>,
        FlowSink::IdType: Send + GenerateId,
        Compact: Sync + Send + 'static,
        Encode: Codec<Current, Compact = Compact, Error = CodecError> + Send + Sync,
        CodecError: Send + Sync + std::error::Error + 'static,
        E: Into<BoxDynError>,
        Encode: Codec<O, Compact = Compact, Error = CodecError> + 'static,
        <FlowSink::Context as MetadataExt<WorkflowRequest>>::Error:
            std::error::Error + Sync + Send + 'static,
        FlowSink: WeakTaskSink<O>,
    {
        self.add_step::<_, O, _, _>(ThenStep {
            inner: task_fn::<F, Current, FlowSink::Context, FnArgs>(then),
            _marker: PhantomData,
        })
    }
}
