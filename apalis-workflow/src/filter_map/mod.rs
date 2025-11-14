use std::marker::PhantomData;

use apalis_core::{
    backend::{BackendExt, TaskSinkError, WaitForCompletion, codec::Codec},
    error::BoxDynError,
    task::{Task, builder::TaskBuilder, metadata::MetadataExt, task_id::TaskId},
    task_fn::{TaskFn, task_fn},
};
use futures::{FutureExt, Sink, StreamExt, future::BoxFuture};
use serde::{Deserialize, Serialize};
use tower::Service;

use crate::{
    SteppedService,
    context::{StepContext, WorkflowContext},
    id_generator::GenerateId,
    router::{GoTo, StepResult, WorkflowRouter},
    service::handle_step_result,
    step::{Layer, Stack, Step},
    workflow::Workflow,
};

/// A layer that filters and maps task inputs to outputs.
#[derive(Clone, Debug)]
pub struct FilterMap<F, I> {
    filter_map: F,
    _marker: PhantomData<I>,
}

impl<F, I> FilterMap<F, I> {
    /// Creates a new `FilterMap` layer with the given filter and map function.
    pub fn new(filter_map: F) -> Self {
        Self {
            filter_map,
            _marker: PhantomData,
        }
    }
}

/// The filter map step that applies filtering and mapping to task inputs.
#[derive(Clone, Debug)]
pub struct FilterMapStep<F, S, I> {
    filter_map: F,
    step: S,
    _marker: PhantomData<I>,
}

impl<S, F, I> Layer<S> for FilterMap<F, I>
where
    F: Clone,
{
    type Step = FilterMapStep<F, S, I>;

    fn layer(&self, step: S) -> Self::Step {
        FilterMapStep {
            filter_map: self.filter_map.clone(),
            step,
            _marker: PhantomData,
        }
    }
}

/// The filter service that handles filtering and mapping of task inputs to outputs.
#[derive(Clone, Debug)]
pub struct FilterService<F, Backend, Input, Iter> {
    service: F,
    _marker: PhantomData<(Backend, Input, Iter)>,
}

/// The state of the filter operation
#[derive(Debug, Clone, Deserialize, Serialize)]

pub enum FilterState {
    /// Unknown collector state
    Unknown,
    /// Collector state to process a single step
    SingleStep,
    /// Collector state to gather results
    Collector,
}

/// The context for the filter operation
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FilterContext<IdType> {
    task_ids: Vec<TaskId<IdType>>,
}

impl<F, B, Input, CodecError, Err, MetaErr, Output, IdType, Iter>
    Service<Task<B::Compact, B::Context, IdType>> for FilterService<F, B, Input, Iter>
where
    F: Service<Task<Input, B::Context, IdType>, Response = Option<Output>>,
    B: BackendExt<Error = Err, IdType = IdType>
        + Send
        + Sync
        + 'static
        + Clone
        + Sink<Task<B::Compact, B::Context, IdType>, Error = Err>
        + WaitForCompletion<GoTo<StepResult<Option<Output>, IdType>>>
        + Unpin,
    B::Context: MetadataExt<FilterState>,
    B::Codec: Codec<Vec<Input>, Error = CodecError, Compact = B::Compact>
        + Codec<Iter, Error = CodecError, Compact = B::Compact>
        + Codec<F::Response, Error = CodecError, Compact = B::Compact>
        + Codec<Input, Error = CodecError, Compact = B::Compact>
        + Codec<Vec<Output>, Error = CodecError, Compact = B::Compact>
        + 'static,
    IdType: GenerateId + Send + 'static + Clone,
    B::Context: MetadataExt<WorkflowContext, Error = MetaErr>
        + MetadataExt<FilterContext<IdType>, Error = MetaErr>
        + Send
        + Sync
        + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    CodecError: std::error::Error + Send + Sync + 'static,
    F::Error: Into<BoxDynError> + Send + 'static,
    MetaErr: std::error::Error + Send + Sync + 'static,
    F::Future: Send + 'static,
    B::Compact: Send + 'static,
    Input: Send + 'static,
    Output: Send + 'static,
    Iter: IntoIterator<Item = Input> + Send + 'static,
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
        let filter_state: FilterState = request.parts.ctx.extract().unwrap_or(FilterState::Unknown);
        let mut ctx = request.parts.data.get::<StepContext<B>>().cloned().unwrap();
        use futures::SinkExt;
        match filter_state {
            FilterState::Unknown => {
                // Handle unknown state
                async move {
                    let main_args: Vec<Input> = vec![];
                    let steps: Task<Iter, _, _> = request.try_map(|arg| B::Codec::decode(&arg))?;
                    let steps = steps.args.into_iter().collect::<Vec<_>>();
                    println!("Decoded steps: {:?}", steps.len());
                    let mut task_ids = Vec::new();
                    for step in steps {
                        let task_id = TaskId::new(B::IdType::generate());

                        let task = TaskBuilder::new(B::Codec::encode(&step)?)
                            .meta(WorkflowContext {
                                step_index: ctx.current_step,
                            })
                            .with_task_id(task_id.clone())
                            .meta(FilterState::SingleStep)
                            .build();
                        ctx.backend
                            .send(task)
                            .await
                            .map_err(|e| TaskSinkError::PushError(e))?;

                        task_ids.push(task_id);
                    }
                    let task_id = TaskId::new(B::IdType::generate());
                    let task = TaskBuilder::new(B::Codec::encode(&main_args)?)
                        .with_task_id(task_id.clone())
                        .meta(WorkflowContext {
                            step_index: ctx.current_step,
                        })
                        .meta(FilterContext { task_ids })
                        .meta(FilterState::Collector)
                        .build();

                    ctx.backend
                        .send(task)
                        .await
                        .map_err(|e| TaskSinkError::PushError(e))?;

                    Ok(GoTo::Done)
                }
                .boxed()
            }
            FilterState::SingleStep => {
                let step: Task<Input, _, _> =
                    request.try_map(|arg| B::Codec::decode(&arg)).unwrap();
                let fut = self.service.call(step);
                async move {
                    let res = fut.await.map_err(|e| e.into())?;
                    Ok(GoTo::Break(StepResult {
                        result: B::Codec::encode(&res)
                            .map_err(|e| TaskSinkError::CodecError::<Err>(e.into()))?,
                        next_task_id: None,
                    }))
                }
                .boxed()
            }
            FilterState::Collector => {
                // Handle collector state
                async move {
                    let filter_ctx: FilterContext<B::IdType> = request.parts.ctx.extract()?;
                    let res: Vec<Output> = ctx
                        .backend
                        .wait_for(filter_ctx.task_ids)
                        .collect::<Vec<_>>()
                        .await
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()?
                        .into_iter()
                        .filter_map(|res| {
                            let res = res.take().ok();
                            match res {
                                Some(GoTo::Break(val)) => val.result,
                                _ => None,
                            }
                        })
                        .collect();
                    if res.is_empty() {
                        return Ok(GoTo::Break(StepResult {
                            result: B::Codec::encode(&res)
                                .map_err(|e| TaskSinkError::CodecError::<Err>(e.into()))?,
                            next_task_id: None,
                        }));
                    }

                    let next = handle_step_result(&mut ctx, GoTo::Next(res)).await?;
                    Ok(next)
                }
                .boxed()
            }
        }
    }
}

impl<F, Input, S, B, CodecError, SinkError, I, Output: 'static, MetaErr, IdType> Step<I, B>
    for FilterMapStep<F, S, I>
where
    I: IntoIterator<Item = Input> + Send + 'static,
    B: BackendExt<Error = SinkError, IdType = IdType>
        + Send
        + Sync
        + 'static
        + Clone
        + Sink<Task<B::Compact, B::Context, IdType>, Error = SinkError>
        + WaitForCompletion<GoTo<StepResult<Option<Output>, IdType>>>
        + Unpin,
    F: Service<Task<Input, B::Context, IdType>, Error = BoxDynError, Response = Option<Output>>
        + Send
        + 'static
        + Clone,
    S: Step<Vec<Output>, B>,
    Input: Send + 'static,
    F::Future: Send + 'static,
    F::Error: Into<BoxDynError> + Send + 'static,
    B::Codec: Codec<F::Response, Error = CodecError, Compact = B::Compact>
        + Codec<Input, Error = CodecError, Compact = B::Compact>
        + 'static,
    CodecError: std::error::Error + Send + Sync + 'static,
    B::IdType: GenerateId + Send + 'static + Clone,
    S::Response: Send + 'static,
    B::Compact: Send + 'static,
    B::Context: Send
        + MetadataExt<WorkflowContext>
        + MetadataExt<FilterState>
        + MetadataExt<FilterContext<B::IdType>>
        + 'static,
    SinkError: std::error::Error + Send + Sync + 'static,
    F::Response: Send + 'static,
    B::Context: MetadataExt<FilterState>,
    B::Codec: Codec<Vec<Input>, Error = CodecError, Compact = B::Compact>
        + Codec<I, Error = CodecError, Compact = B::Compact>
        + Codec<F::Response, Error = CodecError, Compact = B::Compact>
        + Codec<Input, Error = CodecError, Compact = B::Compact>
        + Codec<Vec<Output>, Error = CodecError, Compact = B::Compact>
        + 'static,
    B::IdType: GenerateId + Send + 'static,
    B::Context: MetadataExt<WorkflowContext, Error = MetaErr>
        + MetadataExt<FilterContext<B::IdType>, Error = MetaErr>
        + Send
        + Sync
        + 'static,
    CodecError: std::error::Error + Send + Sync + 'static,
    MetaErr: std::error::Error + Send + Sync + 'static,
    F::Future: Send + 'static,
    B::Compact: Send + 'static,
    Input: Send + 'static,
    Output: Send + 'static,
{
    type Response = Vec<F::Response>;
    type Error = F::Error;
    fn register(&mut self, ctx: &mut WorkflowRouter<B>) -> Result<(), BoxDynError> {
        let svc = SteppedService::new(FilterService {
            service: self.filter_map.clone(),
            _marker: PhantomData::<(B, Input, I)>,
        });
        let count = ctx.steps.len();
        ctx.steps.insert(count, svc);
        self.step.register(ctx)
    }
}

impl<Start, C, L, I: IntoIterator<Item = C>, B: BackendExt> Workflow<Start, I, B, L> {
    /// Adds a filter and map step to the workflow.
    pub fn filter_map<F, Output, FnArgs>(
        self,
        filter_map: F,
    ) -> Workflow<Start, Vec<Output>, B, Stack<FilterMap<TaskFn<F, C, B::Context, FnArgs>, I>, L>>
    where
        TaskFn<F, C, B::Context, FnArgs>:
            Service<Task<C, B::Context, B::IdType>, Response = Option<Output>>,
    {
        self.add_step(FilterMap {
            filter_map: task_fn(filter_map),
            _marker: PhantomData,
        })
    }
}
