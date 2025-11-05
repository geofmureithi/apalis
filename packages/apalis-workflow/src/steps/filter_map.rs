use std::{
    fmt::{Debug, Display},
    marker::PhantomData,
    task::{Context, Poll},
};

use apalis_core::{
    backend::{Backend, WaitForCompletion, WeakTaskSink, codec::Codec},
    error::BoxDynError,
    task::{Task, builder::TaskBuilder, metadata::MetadataExt, task_id::TaskId},
    task_fn::{TaskFn, task_fn},
};
use futures::Sink;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tower::Service;

use crate::{
    CompositeService, GenerateId, GoTo, Step, SteppedService, Workflow, WorkflowError,
    WorkflowRequest, context::StepContext, service::handle_workflow_result,
};

pub struct FilterMap<Step, Input, Output> {
    mapper: PhantomData<FilterMapStep<Step, Input, Output>>,
}

impl<Step, Input, Output> Clone for FilterMap<Step, Input, Output> {
    fn clone(&self) -> Self {
        FilterMap {
            mapper: self.mapper.clone(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FilterContext<IdType> {
    task_ids: Vec<TaskId<IdType>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum FilterState {
    Parent,
    Child,
}

impl<S, Input, Output, E, Sink, Compact, Encode, CodecError, MetadataError>
    Step<Vec<Input>, Sink, Encode> for FilterMap<S, Input, Output>
where
    S: Service<Task<Input, Sink::Context, Sink::IdType>, Response = Option<Output>, Error = E>
        + Sync
        + Send,
    Compact: Send + Sync,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    E: Into<BoxDynError>,
    Output: Sync + Send,
    Sink: Sync
        + WeakTaskSink<Option<Output>> // Store the individual outputs as tasks
        + WeakTaskSink<Vec<Output>> // Store the final vector of outputs to pass down
        + Unpin
        + Send
        + WaitForCompletion<GoTo<Option<Output>>>,
    Input: Send + Sync,
    Sink::Context: Send
        + Sync
        + MetadataExt<WorkflowRequest, Error = MetadataError>
        + MetadataExt<FilterContext<Sink::IdType>, Error = MetadataError>
        + Default,
    Sink::Error: Into<BoxDynError> + Sync + Send + 'static,
    Sink::IdType: GenerateId + Send + Sync,
    Encode: Codec<Vec<Input>, Compact = Compact, Error = CodecError>,
    Encode: Codec<Input, Compact = Compact, Error = CodecError>,
    MetadataError: Into<BoxDynError>,
    CodecError: Into<BoxDynError>,
    Encode: Send + Sync,
{
    type Response = Vec<Output>;
    type Error = WorkflowError;

    async fn run(
        &mut self,
        ctx: &StepContext<Sink, Encode>,
        steps: Task<Vec<Input>, Sink::Context, Sink::IdType>,
    ) -> Result<GoTo<Vec<Output>>, Self::Error> {
        let filter_ctx: FilterContext<Sink::IdType> = steps
            .parts
            .ctx
            .extract()
            .map_err(|e: MetadataError| WorkflowError::MetadataError(e.into()))?;
        let res: Vec<Output> = ctx
            .wait_for(&filter_ctx.task_ids)
            .await
            .map_err(|e| WorkflowError::SingleStepError(e.into()))?
            .into_iter()
            .filter_map(|res| {
                let res = res
                    .take()
                    .map_err(|e| WorkflowError::SingleStepError(e.into()))
                    .ok();
                match res {
                    Some(GoTo::Break(val)) => val,
                    _ => None,
                }
            })
            .collect();
        if res.is_empty() {
            return Ok(GoTo::Break(res));
        }
        Ok(GoTo::Next(res))
    }
}

pub struct FilterMapStep<S, T, O> {
    inner: S,
    _marker: std::marker::PhantomData<(T, O)>,
}

impl<S: Clone, T, O> Clone for FilterMapStep<S, T, O> {
    fn clone(&self) -> Self {
        FilterMapStep {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, T, O> FilterMapStep<S, T, O> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S, T, O, E, B, Compact, Encode> Step<T, B, Encode> for FilterMapStep<S, T, O>
where
    S: Service<Task<T, B::Context, B::IdType>, Response = Option<O>, Error = E> + Sync + Send,
    T: Sync,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    E: Into<BoxDynError>,
    O: Sync + Send,
    B: Sync + Send + 'static + WeakTaskSink<Option<O>>,
    T: Send,
    B::Context: Send + Sync,
    B::IdType: Send,
    Compact: Send + Sync,
    Encode: Codec<T, Compact = Compact> + Sync,
{
    type Response = S::Response;
    type Error = BoxDynError;

    async fn run(
        &mut self,
        _: &StepContext<B, Encode>,
        args: Task<T, B::Context, B::IdType>,
    ) -> Result<GoTo<S::Response>, Self::Error> {
        let res = self.inner.call(args).await.map_err(|e| e.into())?;
        Ok(GoTo::Break(res))
    }
}

pub struct FilterService<Step, Current, FlowSink, Encode, Output, F, FnArgs> {
    step: Step,
    _marker: PhantomData<(Current, FlowSink, Encode, Output, F, FnArgs)>,
}

impl<
    Current,
    S,
    Encode,
    Compact,
    FlowSink,
    Output,
    F: 'static,
    FnArgs: 'static,
    SvcError,
    MetadataError,
    CodecError,
    DbError,
> Service<Task<Compact, FlowSink::Context, FlowSink::IdType>>
    for FilterService<S, Current, FlowSink, Encode, Output, F, FnArgs>
where
    S: Step<Current, FlowSink, Encode, Response = Option<Output>> + Clone + Send + Sync + 'static,
    Encode: Codec<Current, Compact = Compact, Error = CodecError>
        + Codec<Option<Output>, Compact = Compact, Error = CodecError>
        + Codec<Vec<Current>, Compact = Compact, Error = CodecError>
        + Codec<Vec<Output>, Compact = Compact, Error = CodecError>
        + Codec<GoTo<Option<Output>>, Compact = Compact, Error = CodecError>
        + Codec<GoTo<Vec<Output>>, Compact = Compact, Error = CodecError>,
    Output: Send + Sync + 'static,
    S::Error: Into<BoxDynError> + Send + 'static,
    FlowSink: Clone + Send + 'static + Sync + WaitForCompletion<GoTo<Option<Output>>>,
    Current: Send + 'static + Sync,
    FlowSink::Context: Send
        + 'static
        + MetadataExt<FilterContext<FlowSink::IdType>, Error = MetadataError>
        + MetadataExt<FilterState, Error = MetadataError>
        + MetadataExt<WorkflowRequest, Error = MetadataError>
        + Sync
        + Default,
    FlowSink: WeakTaskSink<Option<Output>, Codec = Encode, Error = DbError>
        + WeakTaskSink<Vec<Output>, Codec = Encode, Error = DbError>
        + Sink<Task<Compact, FlowSink::Context, FlowSink::IdType>, Error = DbError>
        + Unpin,
    TaskFn<F, Current, FlowSink::Context, FnArgs>: Service<
            Task<Current, FlowSink::Context, FlowSink::IdType>,
            Response = Option<Output>,
            Error = SvcError,
        > + Send
        + Sync,
    SvcError: Into<BoxDynError> + Send + Sync + 'static,
    MetadataError: Into<BoxDynError> + Send + Sync,
    CodecError: std::error::Error + Sync + Send + 'static,
    DbError: std::error::Error + Sync + Send + 'static,
    <TaskFn<F, Current, FlowSink::Context, FnArgs> as Service<
        Task<Current, FlowSink::Context, FlowSink::IdType>,
    >>::Future: Send + 'static,
    DbError: Into<BoxDynError> + Send + Sync,
    Compact: Send + Sync + 'static,
    FlowSink::IdType: GenerateId + Send + Sync + Display,
    Encode: Send + Sync + 'static,
    S::Error: Into<BoxDynError> + Send + Sync,
{
    type Response = Compact;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: Task<Compact, FlowSink::Context, FlowSink::IdType>) -> Self::Future {
        let mut ctx: StepContext<FlowSink, Encode> = req.parts.data.get_checked().cloned().unwrap();
        let filter_ctx: Result<FilterState, _> = req.parts.ctx.extract();
        match filter_ctx {
            Ok(FilterState::Parent) => {
                let mut step = FilterMap {
                    mapper: PhantomData::<
                        FilterMapStep<
                            TaskFn<F, Current, FlowSink::Context, FnArgs>,
                            Current,
                            Output,
                        >,
                    >,
                };
                Box::pin(async move {
                    let req = req
                        .try_map(|arg| Encode::decode(&arg))
                        .map_err(|e: CodecError| WorkflowError::CodecError(e.into()))?;
                    let res = step.run(&ctx, req).await?;
                    handle_workflow_result::<Vec<Output>, Compact, FlowSink, DbError>(
                        &mut ctx, &res,
                    )
                    .await?;
                    let compact =
                        Encode::encode(&res).map_err(|e| WorkflowError::CodecError(e.into()))?;
                    Ok(compact)
                })
            }
            Ok(FilterState::Child) => {
                let mut step = self.step.clone();
                Box::pin(async move {
                    let req = req
                        .try_map(|arg| Encode::decode(&arg))
                        .map_err(|e: CodecError| WorkflowError::CodecError(e.into()))?;
                    let res = step
                        .run(&ctx, req)
                        .await
                        .map_err(|e| WorkflowError::SingleStepError(e.into()))?;

                    handle_workflow_result::<Option<Output>, Compact, FlowSink, DbError>(
                        &mut ctx, &res,
                    )
                    .await?;
                    let compact =
                        Encode::encode(&res).map_err(|e| WorkflowError::CodecError(e.into()))?;
                    Ok(compact)
                })
            }
            Err(_) => {
                let mut backend = ctx.sink.clone();
                // Assume this is a fresh task, setup the context for parent
                Box::pin(async move {
                    let main_args: Vec<Current> = vec![];
                    let steps: Task<Vec<Current>, _, _> =
                        req.try_map(|arg| Encode::decode(&arg))
                            .map_err(|e: CodecError| WorkflowError::CodecError(e.into()))?;
                    let mut task_ids = Vec::new();
                    for step in steps.args {
                        let task_id = TaskId::new(FlowSink::IdType::generate());

                        let task = TaskBuilder::new(step)
                            .meta(WorkflowRequest {
                                step_index: ctx.current_step,
                            })
                            .with_task_id(task_id.clone())
                            .meta(FilterState::Child)
                            .build();
                        backend
                            .push_task(task)
                            .await
                            .map_err(|e| WorkflowError::SinkError(e.into()))?;

                        task_ids.push(task_id);
                    }
                    let task_id = TaskId::new(FlowSink::IdType::generate());
                    let task = TaskBuilder::new(main_args)
                        .with_task_id(task_id.clone())
                        .meta(WorkflowRequest {
                            step_index: ctx.current_step,
                        })
                        .meta(FilterContext { task_ids })
                        .meta(FilterState::Parent)
                        .build();

                    backend
                        .push_task(task)
                        .await
                        .map_err(|e| WorkflowError::SinkError(e.into()))?;

                    let compact =
                        Encode::encode(&GoTo::<Option<Output>>::ContinueAt(task_id.to_string()))
                            .map_err(|e: CodecError| WorkflowError::CodecError(e.into()))?;
                    Ok(compact)
                })
            }
        }
    }
}

impl<Input, Current, FlowSink, Encode, Compact>
    Workflow<Input, Vec<Current>, FlowSink, Encode, Compact, FlowSink::Context, FlowSink::IdType>
where
    Current: Send + 'static,
    FlowSink: Backend,
    FlowSink::Context: MetadataExt<FilterContext<FlowSink::IdType>> + Send + 'static,
{
    /// Adds a `filter_map` step to the workflow, allowing you to filter and map items in the workflow using a predicate function.
    ///
    /// # Example
    /// ```rust,ignore
    /// use apalis_workflow::Workflow;
    /// // Suppose you have a workflow of integers and want to filter even numbers and double them.
    /// let workflow = Workflow::new("the-even-doubler")
    ///     .filter_map(|x: i32| async move { if x % 2 == 0 { Some(x * 2) } else { None } });
    /// // The resulting workflow will only contain doubled even numbers.
    /// ```
    ///
    /// # Returns
    /// Returns a new `Workflow` with the filter_map step added, producing a vector of outputs.
    ///
    /// # Errors
    /// Errors from the predicate function or encoding/decoding are propagated as boxed dynamic errors.
    ///
    /// # Notes
    /// - The predicate function must be `Send`, `Sync`, and `'static`.
    /// - The workflow step is inserted at the end of the current steps.
    /// - This method is intended for advanced workflow composition scenarios.
    pub fn filter_map<F, Output, FnArgs, SvcError, MetadataError, CodecError, DbError>(
        mut self,
        predicate: F,
    ) -> Workflow<Input, Vec<Output>, FlowSink, Encode, Compact, FlowSink::Context, FlowSink::IdType>
    where
        F: Send + 'static + Sync + Clone,
        TaskFn<F, Current, FlowSink::Context, FnArgs>: Service<
                Task<Current, FlowSink::Context, FlowSink::IdType>,
                Response = Option<Output>,
                Error = SvcError,
            >,
        FnArgs: std::marker::Send + 'static,
        Current: std::marker::Send + 'static + Serialize + Sync + Debug,
        FlowSink::Context: Send
            + 'static
            + Sync
            + Default
            + MetadataExt<FilterContext<FlowSink::IdType>, Error = MetadataError>
            + MetadataExt<WorkflowRequest, Error = MetadataError>
            + MetadataExt<FilterState, Error = MetadataError>,
        <TaskFn<F, Current, FlowSink::Context, FnArgs> as Service<
            Task<Current, FlowSink::Context, FlowSink::IdType>,
        >>::Future: Send + 'static,
        Output: Send + 'static + Sync,
        FnArgs: Sync,
        FlowSink: WeakTaskSink<Option<Output>, Codec = Encode, Error = DbError>
            + WeakTaskSink<Vec<Output>, Codec = Encode, Error = DbError>
            + Sink<Task<Compact, FlowSink::Context, FlowSink::IdType>, Error = DbError>
            + Sync
            + Clone
            + Send
            + 'static
            + WaitForCompletion<GoTo<Option<Output>>>
            + Unpin,
        DbError: Debug,
        SvcError: Send + Sync + 'static + Into<BoxDynError>,
        FlowSink::IdType: Send,
        Encode: Codec<Current, Compact = Compact, Error = CodecError>
            + Codec<GoTo<Option<Output>>, Compact = Compact, Error = CodecError>
            + Codec<Option<Output>, Compact = Compact, Error = CodecError>
            + Codec<Vec<Current>, Compact = Compact, Error = CodecError>
            + Codec<Vec<Output>, Compact = Compact, Error = CodecError>
            + Codec<GoTo<Vec<Output>>, Compact = Compact, Error = CodecError>,
        Compact: Send + Sync + 'static,
        DbError: Into<BoxDynError> + Send + Sync,
        Encode: Send + Sync + 'static,
        CodecError: std::error::Error + Sync + Send + 'static,
        MetadataError: std::error::Error + Sync + Send + 'static,
        FlowSink::IdType: GenerateId + Sync + Send,
        DbError: std::error::Error + Sync + Send + 'static,
        FlowSink::IdType: Display,
    {
        self.steps.insert(self.steps.len(), {
            let svc = SteppedService::<Compact, FlowSink::Context, FlowSink::IdType>::new(
                FilterService {
                    step: FilterMapStep {
                        _marker: PhantomData,
                        inner: task_fn(predicate),
                    },
                    _marker: PhantomData::<(Current, FlowSink, Encode, Output, F, FnArgs)>,
                },
            );
            CompositeService {
                svc,
                _marker: PhantomData,
            }
        });
        Workflow {
            name: self.name,
            steps: self.steps,
            _marker: PhantomData,
        }
    }
}
