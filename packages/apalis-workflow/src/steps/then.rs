use std::{fmt::Debug, marker::PhantomData};

use apalis_core::{
    backend::{codec::Codec, TaskSink},
    error::BoxDynError,
    service_fn::{service_fn, ServiceFn},
    task::{metadata::MetadataExt, Task},
};
use futures::{channel::mpsc::SendError, FutureExt};
use tower::Service;

use crate::{context::StepContext, Step, WorkFlow, WorkflowError, WorkflowRequest};

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
    S: Service<Task<Current, FlowSink::Meta, FlowSink::IdType>, Response = O, Error = E>
        + Sync
        + Send,
    Current: Sync + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    E: Into<BoxDynError>,
    O: Sync,
    FlowSink: Sync + Unpin + TaskSink<Compact> + Send,
    Current: Send,
    FlowSink::Meta: Send + Sync + Default + MetadataExt<WorkflowRequest>,
    FlowSink::Error: Into<BoxDynError> + Send + 'static,
    FlowSink::IdType: Default + Send,
    Compact: Sync + Send,
    Encode: Codec<Current, Compact = Compact> + Sync + Send + 'static,
    Encode::Error: std::error::Error + Sync + Send + 'static,
    <FlowSink::Meta as MetadataExt<WorkflowRequest>>::Error:
        std::error::Error + Sync + Send + 'static,
{
    type Response = S::Response;
    type Error = WorkflowError;
    async fn pre(
        ctx: &mut StepContext<FlowSink, Encode>,
        step: &Current,
    ) -> Result<(), Self::Error> {
        ctx.push_step(step).await?;
        Ok(())
    }

    async fn run(
        &mut self,
        ctx: &StepContext<FlowSink, Encode>,
        args: Task<Current, FlowSink::Meta, FlowSink::IdType>,
    ) -> Result<Self::Response, Self::Error> {
        let res = self
            .inner
            .call(args)
            .await
            .map_err(|e| WorkflowError::SingleStepError(e.into()))?;
        Ok(res)
    }
}

impl<Input, Current, FlowSink, Encode, Compact> WorkFlow<Input, Current, FlowSink, Encode, Compact>
where
    Current: Send + 'static,
    FlowSink: Send + Clone + Sync + 'static + Unpin + TaskSink<Compact>,
{
    pub fn then<F, O, E, FnArgs, CodecError>(
        self,
        then: F,
    ) -> WorkFlow<Input, O, FlowSink, Encode, Compact>
    where
        O: Sync + Send + 'static,
        E: Into<BoxDynError> + Send + Sync + 'static,
        F: Send + 'static + Sync + Clone,
        ServiceFn<F, Current, FlowSink::Meta, FnArgs>:
            Service<Task<Current, FlowSink::Meta, FlowSink::IdType>, Response = O, Error = E>,
        FnArgs: std::marker::Send + 'static + Sync,
        Current: std::marker::Send + 'static + Sync,
        FlowSink::Meta: Send + Sync + Default + 'static + MetadataExt<WorkflowRequest>,
        FlowSink::Error: Into<BoxDynError> + Send + 'static,
        <ServiceFn<F, Current, FlowSink::Meta, FnArgs> as Service<
            Task<Current, FlowSink::Meta, FlowSink::IdType>,
        >>::Future: Send + 'static,
        <ServiceFn<F, Current, FlowSink::Meta, FnArgs> as Service<
            Task<Current, FlowSink::Meta, FlowSink::IdType>,
        >>::Error: Into<BoxDynError>,
        FlowSink::IdType: Send + Default,
        Compact: Sync + Send + 'static,
        Encode: Codec<Current, Compact = Compact, Error = CodecError> + Send + Sync,
        CodecError: Send + Sync + std::error::Error + 'static,
        E: Into<BoxDynError>,
        Encode: Codec<O, Compact = Compact, Error = CodecError> + 'static,
        <FlowSink::Meta as MetadataExt<WorkflowRequest>>::Error:
            std::error::Error + Sync + Send + 'static,
    {
        self.add_step::<_, O, _, _>(ThenStep {
            inner: service_fn::<F, Current, FlowSink::Meta, FnArgs>(then),
            _marker: PhantomData,
        })
    }
}
