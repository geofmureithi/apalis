use std::{
    fmt::Debug,
    marker::PhantomData,
    process::Output,
    sync::Arc,
    task::{Context, Poll},
};

use apalis_core::{
    backend::{codec::Codec, TaskSink, WaitForCompletion},
    error::{BoxDynError, WorkerError},
    task_fn::{service_fn, TaskFn},
    task::{
        builder::TaskBuilder,
        metadata::MetadataExt,
        task_id::{RandomId, TaskId},
        Task,
    },
};
use futures::future::BoxFuture;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use tower::Service;

use crate::{
    context::StepContext, CompositeService, Step, SteppedService, WorkFlow, WorkflowError,
    WorkflowRequest,
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

impl<S, Input, Output, E, Sink, Compact, Encode, CodecError, MetadataError>
    Step<Vec<Input>, Sink, Encode> for FilterMap<S, Input, Output>
where
    S: Service<Task<Input, Sink::Meta, Sink::IdType>, Response = Option<Output>, Error = E>
        + Sync
        + Send,
    Compact: Send + Sync,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    E: Into<BoxDynError>,
    Output: Sync + Send,
    Sink: Sync + TaskSink<Compact> + Unpin + Send + WaitForCompletion<Output, Compact>,
    Input: Send + Sync,
    Sink::Meta: Send
        + Sync
        + MetadataExt<WorkflowRequest, Error = MetadataError>
        + MetadataExt<FilterContext<Sink::IdType>, Error = MetadataError>
        + Default,
    Sink::Error: Into<BoxDynError> + Sync + Send + 'static,
    Sink::IdType: Default + Send + Sync,
    Encode: Codec<Vec<Input>, Compact = Compact, Error = CodecError>,
    Encode: Codec<Input, Compact = Compact, Error = CodecError>,
    MetadataError: Into<BoxDynError>,
    CodecError: Into<BoxDynError>,
    Encode: Send + Sync,
{
    type Response = Vec<Output>;
    type Error = WorkflowError;
    async fn pre(
        ctx: &mut StepContext<Sink, Encode>,
        steps: &Vec<Input>,
    ) -> Result<(), Self::Error> {
        let mut task_ids = Vec::new();
        for step in steps {
            let task_id = ctx.push_next_step(step).await?;
            task_ids.push(task_id);
        }
        let mut meta = Sink::Meta::default();
        meta.inject(FilterContext { task_ids })
            .map_err(|e| WorkflowError::MetadataError(e.into()))?;
        meta.inject(WorkflowRequest {
            step_index: ctx.current_step + 1,
        })
        .map_err(|e| WorkflowError::MetadataError(e.into()))?;
        let task = TaskBuilder::new_with_metadata(
            Encode::encode(steps).map_err(|e| WorkflowError::CodecError(e.into()))?,
            meta,
        )
        .build();

        ctx.push_compact_task(task).await?;
        Ok(())
    }

    async fn run(
        &mut self,
        ctx: &StepContext<Sink, Encode>,
        steps: Task<Vec<Input>, Sink::Meta, Sink::IdType>,
    ) -> Result<Self::Response, Self::Error> {
        let filter_ctx: FilterContext<Sink::IdType> = steps
            .ctx
            .metadata
            .extract()
            .map_err(|e: MetadataError| WorkflowError::MetadataError(e.into()))?;
        let res: Vec<Output> = ctx
            .wait_for(&filter_ctx.task_ids)
            .await
            .map_err(|e| WorkflowError::SingleStepError(e.into()))?
            .into_iter()
            .filter_map(|res| {
                res.result
                    .map_err(|e| WorkflowError::SingleStepError(e.into()))
                    .ok()
            })
            .collect();
        if res.is_empty() {
            return Err(WorkflowError::SingleStepError(
                "FilterMap resulted in no items".into(),
            ));
        }
        Ok(res)
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
    S: Service<Task<T, B::Meta, B::IdType>, Response = Option<O>, Error = E> + Sync + Send,
    T: Sync,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    E: Into<BoxDynError>,
    O: Sync + Send,
    B: Sync + Send + TaskSink<Compact>,
    T: Send,
    B::Meta: Send + Sync,
    B::IdType: Send,
    Compact: Send + Sync,
    Encode: Codec<T, Compact = Compact> + Sync,
{
    type Response = S::Response;
    type Error = BoxDynError;

    async fn run(
        &mut self,
        ctx: &StepContext<B, Encode>,
        args: Task<T, B::Meta, B::IdType>,
    ) -> Result<Self::Response, Self::Error> {
        let res = self.inner.call(args).await.map_err(|e| e.into())?;
        Ok(res)
    }
    async fn post(
        &self,
        ctx: &StepContext<B, Encode>,
        res: &Self::Response,
    ) -> Result<bool, Self::Error> {
        Ok(false) // The parent task will handle the collection
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
    > Service<Task<Compact, FlowSink::Meta, FlowSink::IdType>>
    for FilterService<S, Current, FlowSink, Encode, Output, F, FnArgs>
where
    S: Step<Current, FlowSink, Encode, Response = Option<Output>> + Clone + Send + Sync + 'static,
    Encode: Codec<Current, Compact = Compact, Error = CodecError>
        + Codec<Option<Output>, Compact = Compact, Error = CodecError>
        + Codec<Vec<Current>, Compact = Compact, Error = CodecError>
        + Codec<Vec<Output>, Compact = Compact, Error = CodecError>,
    Output: Send + Sync + 'static,
    S::Error: Into<BoxDynError> + Send + 'static,
    FlowSink: Clone + Send + 'static + Sync + WaitForCompletion<Output, Compact>,
    Current: Send + 'static + Sync,
    FlowSink::Meta: Send
        + 'static
        + MetadataExt<FilterContext<FlowSink::IdType>, Error = MetadataError>
        + MetadataExt<WorkflowRequest, Error = MetadataError>
        + Sync
        + Default,
    FlowSink: TaskSink<Compact> + Unpin,
    TaskFn<F, Current, FlowSink::Meta, FnArgs>: Service<
            Task<Current, FlowSink::Meta, FlowSink::IdType>,
            Response = Option<Output>,
            Error = SvcError,
        > + Send
        + Sync,
    SvcError: Into<BoxDynError> + Send + Sync + 'static,
    MetadataError: Into<BoxDynError> + Send + Sync,
    CodecError: std::error::Error + Sync + Send + 'static,
    <TaskFn<F, Current, FlowSink::Meta, FnArgs> as Service<
        Task<Current, FlowSink::Meta, FlowSink::IdType>,
    >>::Future: Send + 'static,
    FlowSink::Error: Into<BoxDynError> + Send + Sync,
    Compact: Send + Sync + 'static,
    FlowSink::IdType: Default + Send + Sync,
    Encode: Send + Sync + 'static,
    S::Error: Into<BoxDynError> + Send + Sync,
{
    type Response = (bool, Compact);
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: Task<Compact, FlowSink::Meta, FlowSink::IdType>) -> Self::Future {
        let mut ctx: StepContext<FlowSink, Encode> = req.get().cloned().unwrap();
        let filter_ctx: Result<FilterContext<FlowSink::IdType>, _> = req.ctx.metadata.extract();
        match filter_ctx {
            Ok(_) => {
                let mut step = FilterMap {
                    mapper: PhantomData::<
                        FilterMapStep<
                            TaskFn<F, Current, FlowSink::Meta, FnArgs>,
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
                    let should_next = step.post(&mut ctx, &res).await?;
                    Ok((
                        should_next,
                        Encode::encode(&res)
                            .map_err(|e: CodecError| WorkflowError::CodecError(e.into()))?,
                    ))
                })
            }
            Err(_) => {
                let mut step = self.step.clone();
                Box::pin(async move {
                    let req = req
                        .try_map(|arg| Encode::decode(&arg))
                        .map_err(|e: CodecError| WorkflowError::CodecError(e.into()))?;
                    let res = step
                        .run(&ctx, req)
                        .await
                        .map_err(|e| WorkflowError::SingleStepError(e.into()))?;
                    let should_next = step
                        .post(&mut ctx, &res)
                        .await
                        .map_err(|e| WorkflowError::SingleStepError(e.into()))?;
                    Ok((
                        should_next,
                        Encode::encode(&res)
                            .map_err(|e: CodecError| WorkflowError::CodecError(e.into()))?,
                    ))
                })
            }
        }
    }
}

impl<Input, Current, FlowSink, Encode, Compact>
    WorkFlow<Input, Vec<Current>, FlowSink, Encode, Compact>
where
    Current: Send + 'static,
    FlowSink: TaskSink<Compact>,
    FlowSink::Meta: MetadataExt<FilterContext<FlowSink::IdType>> + Send + 'static,
{
    /// Adds a `filter_map` step to the workflow, allowing you to filter and map items in the workflow using a predicate function.
    ///
    /// # Example
    /// ```rust
    /// use apalis_workflow::WorkFlow;
    ///
    /// // Suppose you have a workflow of integers and want to filter even numbers and double them.
    /// let workflow = WorkFlow::new()
    ///     .filter_map(|x: i32| if x % 2 == 0 { Some(x * 2) } else { None });
    /// // The resulting workflow will only contain doubled even numbers.
    /// ```
    ///
    /// # Returns
    /// Returns a new `WorkFlow` with the filter_map step added, producing a vector of outputs.
    ///
    /// # Errors
    /// Errors from the predicate function or encoding/decoding are propagated as boxed dynamic errors.
    ///
    /// # Notes
    /// - The predicate function must be `Send`, `Sync`, and `'static`.
    /// - The workflow step is inserted at the end of the current steps.
    /// - This method is intended for advanced workflow composition scenarios.
    pub fn filter_map<F, Output, FnArgs, SvcError, MetadataError, CodecError>(
        mut self,
        predicate: F,
    ) -> WorkFlow<Input, Vec<Output>, FlowSink, Encode, Compact>
    where
        F: Send + 'static + Sync + Clone,
        TaskFn<F, Current, FlowSink::Meta, FnArgs>: Service<
            Task<Current, FlowSink::Meta, FlowSink::IdType>,
            Response = Option<Output>,
            Error = SvcError,
        >,
        FnArgs: std::marker::Send + 'static,
        Current: std::marker::Send + 'static + Serialize + Sync + Debug,
        FlowSink::Meta: Send
            + 'static
            + Sync
            + Default
            + MetadataExt<FilterContext<FlowSink::IdType>, Error = MetadataError>
            + MetadataExt<WorkflowRequest, Error = MetadataError>,
        <TaskFn<F, Current, FlowSink::Meta, FnArgs> as Service<
            Task<Current, FlowSink::Meta, FlowSink::IdType>,
        >>::Future: Send + 'static,
        Output: Send + 'static + Sync,
        FnArgs: Sync,
        FlowSink: Sync + Clone + Send + 'static + WaitForCompletion<Output, Compact> + Unpin,
        FlowSink::Error: Debug,
        SvcError: Send + Sync + 'static + Into<BoxDynError>,
        FlowSink::IdType: Send,
        Encode: Codec<Current, Compact = Compact, Error = CodecError>
            + Codec<Option<Output>, Compact = Compact, Error = CodecError>
            + Codec<Vec<Current>, Compact = Compact, Error = CodecError>
            + Codec<Vec<Output>, Compact = Compact, Error = CodecError>,
        Compact: Send + Sync + 'static,
        FlowSink::Error: Into<BoxDynError> + Send + Sync,
        Encode: Send + Sync + 'static,
        CodecError: std::error::Error + Sync + Send + 'static,
        MetadataError: std::error::Error + Sync + Send + 'static,
        FlowSink::IdType: Default + Sync + Send,
    {
        self.steps.insert(self.steps.len(), {
            let pre_hook = Arc::new(Box::new(
                move |ctx: &StepContext<FlowSink, Encode>, step: &Compact| {
                    let val = Encode::decode(step);
                    match val {
                        Ok(val) => {
                            let mut ctx = ctx.clone();
                            async move {
                                FilterMap::<
                                    TaskFn<F, Current, FlowSink::Meta, FnArgs>,
                                    Current,
                                    Output,
                                >::pre(&mut ctx, &val)
                                .await?;
                                Ok(())
                            }
                            .boxed()
                        }
                        Err(_) => async move { Ok(()) }.boxed(),
                    }
                },
            )
                as Box<
                    dyn Fn(
                            &StepContext<FlowSink, Encode>,
                            &Compact,
                        ) -> BoxFuture<'static, Result<(), BoxDynError>>
                        + Send
                        + Sync
                        + 'static,
                >);

            let svc =
                SteppedService::<Compact, FlowSink::Meta, FlowSink::IdType>::new(FilterService {
                    step: FilterMapStep {
                        _marker: PhantomData,
                        inner: service_fn(predicate),
                    },
                    _marker: PhantomData::<(Current, FlowSink, Encode, Output, F, FnArgs)>,
                });
            CompositeService { pre_hook, svc }
        });
        WorkFlow {
            name: self.name,
            steps: self.steps,
            _marker: PhantomData,
        }
    }
}
