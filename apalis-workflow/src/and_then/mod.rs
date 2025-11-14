use std::marker::PhantomData;

use apalis_core::{
    backend::{BackendExt, codec::Codec},
    error::BoxDynError,
    task::{Task, metadata::MetadataExt},
    task_fn::{TaskFn, task_fn},
};
use futures::{
    FutureExt, Sink,
    future::{BoxFuture, ready},
};
use tower::{Service, ServiceBuilder, layer::layer_fn};

use crate::{
    SteppedService,
    context::{StepContext, WorkflowContext},
    id_generator::GenerateId,
    router::{GoTo, StepResult, WorkflowRouter},
    service::handle_step_result,
    step::{Layer, Stack, Step},
    workflow::Workflow,
};

/// A layer that represents an `and_then` step in the workflow.
#[derive(Clone, Debug)]
pub struct AndThen<F> {
    then_fn: F,
}

impl<F> AndThen<F> {
    /// Creates a new `AndThen` layer with the provided function.
    pub fn new(then_fn: F) -> Self {
        Self { then_fn }
    }
}

/// The step implementation for the `AndThen` layer.
#[derive(Clone, Debug)]
pub struct AndThenStep<F, S> {
    then_fn: F,
    step: S,
}

impl<S, F> Layer<S> for AndThen<F>
where
    F: Clone,
{
    type Step = AndThenStep<F, S>;

    fn layer(&self, step: S) -> Self::Step {
        AndThenStep {
            then_fn: self.then_fn.clone(),
            step,
        }
    }
}

impl<F, Input, S, B, CodecError, SinkError> Step<Input, B> for AndThenStep<F, S>
where
    B: BackendExt<Error = SinkError>
        + Send
        + Sync
        + 'static
        + Clone
        + Sink<Task<B::Compact, B::Context, B::IdType>, Error = SinkError>
        + Unpin,
    F: Service<Task<Input, B::Context, B::IdType>, Error = BoxDynError> + Send + 'static + Clone,
    S: Step<F::Response, B>,
    Input: Send + 'static,
    F::Future: Send + 'static,
    F::Error: Into<BoxDynError> + Send + 'static,
    B::Codec: Codec<F::Response, Error = CodecError, Compact = B::Compact>
        + Codec<Input, Error = CodecError, Compact = B::Compact>
        + Codec<S::Response, Error = CodecError, Compact = B::Compact>
        + 'static,
    CodecError: std::error::Error + Send + Sync + 'static,
    B::IdType: GenerateId + Send + 'static,
    S::Response: Send + 'static,
    B::Compact: Send + 'static,
    B::Context: Send + MetadataExt<WorkflowContext> + 'static,
    SinkError: std::error::Error + Send + Sync + 'static,
    F::Response: Send + 'static,
{
    type Response = F::Response;
    type Error = F::Error;
    fn register(&mut self, ctx: &mut WorkflowRouter<B>) -> Result<(), BoxDynError> {
        let svc = ServiceBuilder::new()
            .layer(layer_fn(|s| AndThenService {
                service: s,
                _marker: PhantomData::<(B, Input)>,
            }))
            .map_response(|res: F::Response| GoTo::Next(res))
            .service(self.then_fn.clone());
        let svc = SteppedService::<B::Compact, B::Context, B::IdType>::new(svc);
        let count = ctx.steps.len();
        ctx.steps.insert(count, svc);
        self.step.register(ctx)
    }
}

/// The service implementation for the `AndThen` step.
#[derive(Debug)]
pub struct AndThenService<Svc, Backend, Cur> {
    service: Svc,
    _marker: PhantomData<(Backend, Cur)>,
}

impl<Svc, Backend, Cur> AndThenService<Svc, Backend, Cur> {
    /// Creates a new `AndThenService` with the provided service.
    pub fn new(service: Svc) -> Self {
        Self {
            service,
            _marker: PhantomData,
        }
    }
}

impl<S, B, Cur, Res, CodecErr, SinkError> Service<Task<B::Compact, B::Context, B::IdType>>
    for AndThenService<S, B, Cur>
where
    S: Service<Task<Cur, B::Context, B::IdType>, Response = GoTo<Res>>,
    S::Future: Send + 'static,
    B: BackendExt<Error = SinkError>
        + Sync
        + Send
        + 'static
        + Clone
        + Sink<Task<B::Compact, B::Context, B::IdType>, Error = SinkError>
        + Unpin,
    B::Codec: Codec<Cur, Compact = B::Compact, Error = CodecErr>
        + Codec<Res, Compact = B::Compact, Error = CodecErr>,
    S::Error: Into<BoxDynError> + Send + 'static,
    CodecErr: Into<BoxDynError> + Send + 'static,
    Cur: Send + 'static,
    B::IdType: GenerateId + Send + 'static,
    SinkError: std::error::Error + Send + Sync + 'static,
    Res: Send + 'static,
    B::Compact: Send + 'static,
    B::Context: Send + MetadataExt<WorkflowContext> + 'static,
{
    type Response = GoTo<StepResult<B::Compact, B::IdType>>;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, request: Task<B::Compact, B::Context, B::IdType>) -> Self::Future {
        let mut ctx = request.parts.data.get::<StepContext<B>>().cloned().unwrap();
        let compacted = request.try_map(|t| B::Codec::decode(&t));
        match compacted {
            Ok(task) => {
                let fut = self.service.call(task);
                async move {
                    let res = fut.await.map_err(|e| e.into())?;
                    Ok(handle_step_result(&mut ctx, res).await?)
                }
                .boxed()
            }
            Err(e) => ready(Err(e.into())).boxed(),
        }
    }
}

impl<Start, Cur, B, L> Workflow<Start, Cur, B, L>
where
    B: BackendExt,
{
    /// Adds a transformation step to the workflow that processes the output of the previous step.
    ///
    /// The `and_then` method allows you to chain operations by providing a function that
    /// takes the result of the current workflow step and transforms it into the input
    /// for the next step. This enables building complex processing pipelines with
    /// type-safe transformations between steps.
    /// # Example
    /// ```rust,ignore
    /// workflow
    ///     .and_then(extract)
    ///     .and_then(transform)
    ///     .and_then(load);
    /// ```
    pub fn and_then<F, O, FnArgs>(
        self,
        and_then: F,
    ) -> Workflow<Start, O, B, Stack<AndThen<TaskFn<F, Cur, B::Context, FnArgs>>, L>>
    where
        TaskFn<F, Cur, B::Context, FnArgs>: Service<Task<Cur, B::Context, B::IdType>, Response = O>,
    {
        self.add_step(AndThen {
            then_fn: task_fn(and_then),
        })
    }
}
