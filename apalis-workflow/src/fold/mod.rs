use std::{marker::PhantomData, task::Context};

use apalis_core::{
    backend::{BackendExt, TaskSinkError, codec::Codec},
    error::BoxDynError,
    task::{Task, builder::TaskBuilder, metadata::MetadataExt, task_id::TaskId},
    task_fn::{TaskFn, task_fn},
};
use futures::{FutureExt, Sink, SinkExt, future::BoxFuture};
use serde::{Deserialize, Serialize};
use tower::Service;

use crate::{
    SteppedService,
    context::{StepContext, WorkflowContext},
    id_generator::GenerateId,
    router::{GoTo, StepResult, WorkflowRouter},
    step::{Layer, Stack, Step},
    workflow::Workflow,
};

/// The fold layer that folds over a collection of items.
#[derive(Clone, Debug)]
pub struct Fold<F, Init> {
    fold: F,
    _marker: std::marker::PhantomData<Init>,
}

impl<F, Init, S> Layer<S> for Fold<F, Init>
where
    F: Clone,
    Init: Clone,
{
    type Step = FoldStep<S, F, Init>;

    fn layer(&self, step: S) -> Self::Step {
        FoldStep {
            inner: step,
            fold: self.fold.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}
impl<Start, C, L, I: IntoIterator<Item = C>, B: BackendExt> Workflow<Start, I, B, L> {
    /// Folds over a collection of items in the workflow.
    pub fn fold<F, Output, FnArgs, Init>(
        self,
        fold: F,
    ) -> Workflow<Start, Output, B, Stack<Fold<TaskFn<F, (Init, C), B::Context, FnArgs>, Init>, L>>
    where
        TaskFn<F, (Init, C), B::Context, FnArgs>:
            Service<Task<(Init, C), B::Context, B::IdType>, Response = Output>,
    {
        self.add_step(Fold {
            fold: task_fn(fold),
            _marker: PhantomData,
        })
    }
}

/// The fold step that folds over a collection of items.
#[derive(Clone, Debug)]
pub struct FoldStep<S, F, Init> {
    inner: S,
    fold: F,
    _marker: std::marker::PhantomData<Init>,
}

impl<S, F, Input, I: IntoIterator<Item = Input>, Init, B, MetaErr, Err, CodecError> Step<I, B>
    for FoldStep<S, F, Init>
where
    F: Service<Task<(Init, Input), B::Context, B::IdType>, Response = Init>
        + Send
        + 'static
        + Clone,
    S: Step<Init, B>,
    B: BackendExt<Error = Err>
        + Send
        + Sync
        + Clone
        + Sink<Task<B::Compact, B::Context, B::IdType>, Error = Err>
        + Unpin
        + 'static,
    I: IntoIterator<Item = Input> + Send + 'static,
    B::Context: MetadataExt<FoldState, Error = MetaErr>
        + MetadataExt<WorkflowContext, Error = MetaErr>
        + Send
        + 'static,
    B::Codec: Codec<(Init, Vec<Input>), Error = CodecError, Compact = B::Compact>
        + Codec<Init, Error = CodecError, Compact = B::Compact>
        + Codec<I, Error = CodecError, Compact = B::Compact>
        + Codec<(Init, Input), Error = CodecError, Compact = B::Compact>
        + 'static,
    B::IdType: GenerateId + Send + 'static + Clone,
    Init: Default + Send + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    CodecError: std::error::Error + Send + Sync + 'static,
    F::Error: Into<BoxDynError> + Send + 'static,
    MetaErr: std::error::Error + Send + Sync + 'static,
    F::Future: Send + 'static,
    B::Compact: Send + 'static,
    Input: Send + 'static,
{
    type Response = Init;
    type Error = F::Error;
    fn register(&mut self, ctx: &mut WorkflowRouter<B>) -> Result<(), BoxDynError> {
        let svc = SteppedService::new(FoldService {
            fold: self.fold.clone(),
            _marker: PhantomData::<(Init, I, B)>,
        });
        let count = ctx.steps.len();
        ctx.steps.insert(count, svc);
        self.inner.register(ctx)
    }
}

/// The fold service that handles folding over a collection of items.
#[derive(Clone, Debug)]
pub struct FoldService<F, Init, I, B> {
    fold: F,
    _marker: std::marker::PhantomData<(Init, I, B)>,
}

impl<F, Init, I, B> FoldService<F, Init, I, B> {
    /// Creates a new `FoldService` with the given fold function.
    pub fn new(fold: F) -> Self {
        Self {
            fold,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<F, Init, I, B, Input, CodecError, MetaErr, Err>
    Service<Task<B::Compact, B::Context, B::IdType>> for FoldService<F, Init, I, B>
where
    F: Service<Task<(Init, Input), B::Context, B::IdType>, Response = Init>
        + Send
        + 'static
        + Clone,
    B: BackendExt<Error = Err>
        + Send
        + Sync
        + Clone
        + Sink<Task<B::Compact, B::Context, B::IdType>, Error = Err>
        + Unpin
        + 'static,
    I: IntoIterator<Item = Input> + Send + 'static,
    B::Context: MetadataExt<FoldState, Error = MetaErr>
        + MetadataExt<WorkflowContext, Error = MetaErr>
        + Send
        + 'static,
    B::Codec: Codec<(Init, Vec<Input>), Error = CodecError, Compact = B::Compact>
        + Codec<Init, Error = CodecError, Compact = B::Compact>
        + Codec<I, Error = CodecError, Compact = B::Compact>
        + Codec<(Init, Input), Error = CodecError, Compact = B::Compact>
        + 'static,
    B::IdType: GenerateId + Send + 'static,
    Init: Default + Send + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    CodecError: std::error::Error + Send + Sync + 'static,
    F::Error: Into<BoxDynError> + Send + 'static,
    MetaErr: std::error::Error + Send + Sync + 'static,
    F::Future: Send + 'static,
    B::Compact: Send + 'static,
    Input: Send + 'static,
{
    type Response = GoTo<StepResult<B::Compact, B::IdType>>;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.fold.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, task: Task<B::Compact, B::Context, B::IdType>) -> Self::Future {
        let state = task.parts.ctx.extract().unwrap_or(FoldState::Unknown);
        let mut ctx = task.parts.data.get::<StepContext<B>>().cloned().unwrap();
        let mut fold = self.fold.clone();

        match state {
            FoldState::Unknown => async move {
                let task_id = TaskId::new(B::IdType::generate());
                let steps: Task<I, _, _> = task.try_map(|arg| B::Codec::decode(&arg))?;
                let steps = steps.args.into_iter().collect::<Vec<_>>();
                let task = TaskBuilder::new(B::Codec::encode(&(Init::default(), steps))?)
                    .meta(WorkflowContext {
                        step_index: ctx.current_step,
                    })
                    .with_task_id(task_id.clone())
                    .meta(FoldState::Collection)
                    .build();
                ctx.backend
                    .send(task)
                    .await
                    .map_err(|e| TaskSinkError::PushError(e))?;
                Ok(GoTo::Next(StepResult {
                    result: B::Codec::encode(&Init::default())?,
                    next_task_id: Some(task_id),
                }))
            }
            .boxed(),
            FoldState::Collection => async move {
                let args: (Init, Vec<Input>) = B::Codec::decode(&task.args)?;
                let (acc, items) = args;

                let mut items = items.into_iter();
                let next = items.next().unwrap();
                let rest = items.collect::<Vec<_>>();
                let fold_task = task.map(|_| (acc, next));
                let response = fold.call(fold_task).await.map_err(|e| e.into())?;

                match rest.len() {
                    0 if ctx.has_next => {
                        let task_id = TaskId::new(B::IdType::generate());
                        let result = B::Codec::encode(&response)?;
                        let next_step = TaskBuilder::new(result)
                            .with_task_id(task_id.clone())
                            .meta(WorkflowContext {
                                step_index: ctx.current_step + 1,
                            })
                            .build();
                        ctx.backend
                            .send(next_step)
                            .await
                            .map_err(|e| TaskSinkError::PushError(e))?;
                        Ok(GoTo::Break(StepResult {
                            result: B::Codec::encode(&response)?,
                            next_task_id: Some(task_id),
                        }))
                    }
                    0 => Ok(GoTo::Break(StepResult {
                        result: B::Codec::encode(&response)?,
                        next_task_id: None,
                    })),
                    1.. => {
                        // Shouldn't this be limited?
                        let task_id = TaskId::new(B::IdType::generate());
                        let result = B::Codec::encode(&response)?;
                        let steps = TaskBuilder::new(B::Codec::encode(&(response, rest))?)
                            .with_task_id(task_id.clone())
                            .meta(WorkflowContext {
                                step_index: ctx.current_step,
                            })
                            .meta(FoldState::Collection)
                            .build();
                        ctx.backend
                            .send(steps)
                            .await
                            .map_err(|e| TaskSinkError::PushError(e))?;
                        return Ok(GoTo::Next(StepResult {
                            result,
                            next_task_id: Some(task_id),
                        }));
                    }
                }
            }
            .boxed(),
        }
    }
}

/// The state of the fold operation
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum FoldState {
    /// Unknown
    Unknown,
    /// Collection has started
    Collection,
}
