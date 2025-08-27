use std::{fmt::Debug, marker::PhantomData};

use apalis_core::{
    backend::{codec::Codec, TaskSink},
    error::BoxDynError,
    service_fn::{service_fn, ServiceFn},
    task::{metadata::MetadataExt, Task},
};
use futures::{channel::mpsc::SendError, FutureExt};
use tower::Service;

use crate::{context::StepContext, Step, WorkFlow, WorkflowRequest};

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
    E: Into<BoxDynError> + Debug,
    O: Sync,
    FlowSink: Sync + Unpin + TaskSink<Compact> + Send,
    Current: Send + Debug + Clone,
    FlowSink::Meta: Send + Sync + Default + MetadataExt<WorkflowRequest>,
    FlowSink::Error: Debug + Send + 'static,
    // Meta::Error: Debug,
    FlowSink::IdType: Clone + Default + Send,
    Compact: Sync + Clone + Send,
    Encode: Codec<Current, Compact = Compact> + Sync + Send + 'static,
    Encode::Error: Debug + Sync + Send + 'static,
{
    type Response = S::Response;
    type Error = BoxDynError;
    async fn pre(
        ctx: &mut StepContext<FlowSink, Encode>,
        step: &Current,
    ) -> Result<(), Self::Error> {
        ctx.push_step(step).await.unwrap();
        Ok(())
    }

    async fn run(
        &mut self,
        ctx: &StepContext<FlowSink, Encode>,
        args: Task<Current, FlowSink::Meta, FlowSink::IdType>,
    ) -> Result<Self::Response, Self::Error> {
        let res = self.inner.call(args).await.unwrap();
        Ok(res)
    }
}

impl<Input, Current, FlowSink, Encode, Compact> WorkFlow<Input, Current, FlowSink, Encode, Compact>
where
    Current: Send + 'static,
    FlowSink: Send + Clone + Sync + 'static + Unpin + TaskSink<Compact>,
{
    pub fn then<F, O, E, FnArgs>(mut self, then: F) -> WorkFlow<Input, O, FlowSink, Encode, Compact>
    where
        O: Sync + Send + 'static,
        E: Into<BoxDynError> + Send + Sync + 'static,
        F: Send + 'static + Sync + Clone,
        ServiceFn<F, Current, FlowSink::Meta, FnArgs>:
            Service<Task<Current, FlowSink::Meta, FlowSink::IdType>, Response = O, Error = E>,
        FnArgs: std::marker::Send + 'static + Sync,
        Current: std::marker::Send + 'static + Sync + Debug,
        FlowSink::Meta: Send + Sync + Default + 'static + MetadataExt<WorkflowRequest>,
        FlowSink::Error: Debug + Send + 'static,
        <ServiceFn<F, Current, FlowSink::Meta, FnArgs> as Service<
            Task<Current, FlowSink::Meta, FlowSink::IdType>,
        >>::Future: Send + 'static,
        <ServiceFn<F, Current, FlowSink::Meta, FnArgs> as Service<
            Task<Current, FlowSink::Meta, FlowSink::IdType>,
        >>::Error: Into<BoxDynError>,
        FlowSink::IdType: Clone + Send + Default,
        Compact: Sync + Clone + Send + 'static,
        Encode: Codec<Current, Compact = Compact> + Send + Sync + Clone,
        FlowSink::Meta: Clone,
        <Encode as Codec<Current>>::Error: Send + Sync + Debug + 'static,
        <Encode as Codec<O>>::Error: Send + Sync + Debug + 'static,
        E: Debug,
        Current: Clone,
        Encode: Codec<O, Compact = Compact> + 'static,
    {
        self.add_step::<_, O>(ThenStep {
            inner: service_fn::<F, Current, FlowSink::Meta, FnArgs>(then),
            _marker: PhantomData,
        })
    }
}
