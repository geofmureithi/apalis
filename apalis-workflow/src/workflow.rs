use std::marker::PhantomData;

use apalis_core::{
    backend::{Backend, BackendExt, codec::RawDataBackend},
    error::BoxDynError,
    task::{Task, metadata::MetadataExt},
    worker::builder::{IntoWorkerService, WorkerService},
};
use futures::Sink;

use crate::{
    context::WorkflowContext,
    id_generator::GenerateId,
    router::WorkflowRouter,
    service::WorkflowService,
    step::{Identity, Layer, Stack, Step},
};

/// A workflow represents a sequence of steps to be executed in order.
#[derive(Debug)]
pub struct Workflow<Start, Current, Backend, T = Identity> {
    pub(crate) inner: T,
    pub(crate) name: String,
    _marker: PhantomData<(Start, Current, Backend)>,
}

impl<Start, Backend> Workflow<Start, Start, Backend> {
    #[allow(missing_docs)]
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self {
            inner: Identity,
            name: name.to_owned(),
            _marker: PhantomData,
        }
    }
}

impl<Start, Cur, B, L> Workflow<Start, Cur, B, L> {
    /// Adds a new step to the workflow pipeline.
    pub fn add_step<S, Output>(self, step: S) -> Workflow<Start, Output, B, Stack<S, L>> {
        Workflow {
            inner: Stack::new(step, self.inner),
            name: self.name,
            _marker: PhantomData,
        }
    }

    /// Finalizes the workflow by attaching a root step.
    pub fn finalize<S>(self, root: S) -> Workflow<Start, Cur, B, L::Step>
    where
        S: Step<Cur, B>,
        L: Layer<S>,
        B: BackendExt,
    {
        Workflow {
            inner: self.inner.layer(root),
            name: self.name,
            _marker: PhantomData,
        }
    }
}

impl<Start, Cur, B, L> Workflow<Start, Cur, B, L>
where
    B: BackendExt,
{
    /// Builds the workflow by layering the root step.
    pub fn build<N>(self) -> L::Step
    where
        L: Layer<RootStep<N>>,
    {
        let root = RootStep(std::marker::PhantomData);
        self.inner.layer(root)
    }
}

/// The root step of a workflow.
#[derive(Clone, Debug)]
pub struct RootStep<Res>(std::marker::PhantomData<Res>);

impl<Res> Default for RootStep<Res> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<Input, Current, B: BackendExt> Step<Input, B> for RootStep<Current> {
    type Response = Current;
    type Error = BoxDynError;
    fn register(&mut self, _ctx: &mut WorkflowRouter<B>) -> Result<(), BoxDynError> {
        // TODO: Implement runtime checks to ensure Inputs and Outputs are compatible
        Ok(())
    }
}

impl<Input, Output, Current, B, Compact, Err, L>
    IntoWorkerService<B, WorkflowService<B, Output>, Compact, B::Context>
    for Workflow<Input, Current, B, L>
where
    B: BackendExt<Compact = Compact>
        + Send
        + Sync
        + 'static
        + Sink<Task<Compact, B::Context, B::IdType>, Error = Err>
        + Unpin
        + Clone,
    Err: std::error::Error + Send + Sync + 'static,
    B::Context: MetadataExt<WorkflowContext> + Send + Sync + 'static,
    B::IdType: Send + 'static + Default + GenerateId,
    B: Sync + Backend<Args = Compact, Error = Err>,
    B::Compact: Send + Sync + 'static,
    <B::Context as MetadataExt<WorkflowContext>>::Error: Into<BoxDynError>,
    L: Layer<RootStep<Current>>,
    L::Step: Step<Output, B>,
{
    type Backend = RawDataBackend<B>;
    fn into_service(self, b: B) -> WorkerService<RawDataBackend<B>, WorkflowService<B, Output>> {
        let mut ctx = WorkflowRouter::<B>::new();

        let mut root = self.finalize(RootStep(std::marker::PhantomData));

        root.inner
            .register(&mut ctx)
            .expect("Failed to register workflow steps");
        WorkerService {
            backend: RawDataBackend::new(b.clone()),
            service: WorkflowService::new(ctx.steps, b),
        }
    }
}
