use std::time::Duration;

use apalis_core::{
    backend::{BackendExt, codec::Codec},
    error::BoxDynError,
    task::{Task, builder::TaskBuilder, metadata::MetadataExt, task_id::TaskId},
};
use futures::sink::SinkExt;
use futures::{FutureExt, Sink, future::BoxFuture};
use tower::Service;

use crate::{
    SteppedService, Workflow,
    context::{StepContext, WorkflowContext},
    id_generator::GenerateId,
    router::{GoTo, StepResult, WorkflowRouter},
    step::{Layer, Stack, Step},
};

/// Layer that delays execution by a specified duration
#[derive(Clone, Debug)]
pub struct DelayFor {
    duration: Duration,
}

impl<S> Layer<S> for DelayFor
where
    S: Clone,
{
    type Step = DelayForStep<S>;

    fn layer(&self, step: S) -> Self::Step {
        DelayForStep {
            inner: step,
            duration: self.duration,
        }
    }
}

/// Step that delays execution by a specified duration
#[derive(Clone, Debug)]
pub struct DelayForStep<S> {
    inner: S,
    duration: Duration,
}

impl<Input, B, S, Err> Step<Input, B> for DelayForStep<S>
where
    B::IdType: GenerateId + Send + 'static,
    B::Compact: Send + 'static,
    B: Sink<Task<B::Compact, B::Context, B::IdType>, Error = Err>
        + Unpin
        + Send
        + Sync
        + Clone
        + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    S: Clone + Send + 'static,
    S::Response: Send + 'static,
    B::Codec: Codec<Duration, Compact = B::Compact> + Codec<Input, Compact = B::Compact> + 'static,
    <B::Codec as Codec<Duration>>::Error: Into<BoxDynError>,
    B::Context: Send + 'static + MetadataExt<WorkflowContext>,
    Input: Send + 'static,
    <B::Codec as Codec<Input>>::Error: Into<BoxDynError>,
    B: BackendExt,
    S: Step<Input, B>,
{
    type Response = Input;
    type Error = BoxDynError;
    fn register(&mut self, ctx: &mut WorkflowRouter<B>) -> Result<(), BoxDynError> {
        let duration = self.duration;
        let svc = SteppedService::new(DelayWithStep {
            f: Box::new(move |_| duration),
            inner: self.inner.clone(),
            _marker: std::marker::PhantomData,
        });
        let count = ctx.steps.len();
        ctx.steps.insert(count, svc);
        self.inner.register(ctx)
    }
}

/// Step that delays execution by a specified duration
#[derive(Clone, Debug)]
pub struct DelayWith<F, B, Input> {
    f: F,
    _marker: std::marker::PhantomData<(B, Input)>,
}

impl<S, F: Clone, B, I> Layer<S> for DelayWith<F, B, I> {
    type Step = DelayWithStep<S, F, B, I>;

    fn layer(&self, step: S) -> Self::Step {
        DelayWithStep {
            f: self.f.clone(),
            inner: step,
            _marker: std::marker::PhantomData,
        }
    }
}

/// Step that delays execution by a specified duration
#[derive(Clone, Debug)]
pub struct DelayWithStep<S, F, B, Input> {
    f: F,
    inner: S,
    _marker: std::marker::PhantomData<(B, Input)>,
}

impl<Input, F, B, S, Err> Step<Input, B> for DelayWithStep<S, F, B, Input>
where
    F: FnMut(Task<Input, B::Context, B::IdType>) -> Duration + Send + 'static + Clone,
    B::IdType: GenerateId + Send + 'static,
    B::Compact: Send + 'static,
    B: Sink<Task<B::Compact, B::Context, B::IdType>, Error = Err>
        + Unpin
        + Send
        + Sync
        + Clone
        + 'static,
    Err: std::error::Error + Send + Sync + 'static,
    S: Clone + Send + 'static,
    S::Response: Send + 'static,
    B::Codec: Codec<Duration, Compact = B::Compact> + Codec<Input, Compact = B::Compact> + 'static,
    <B::Codec as Codec<Duration>>::Error: Into<BoxDynError>,
    B::Context: Send + 'static + MetadataExt<WorkflowContext>,
    Input: Send + 'static,
    <B::Codec as Codec<Input>>::Error: Into<BoxDynError>,
    B: BackendExt,
    S: Step<Input, B>,
{
    type Response = Input;
    type Error = BoxDynError;
    fn register(&mut self, ctx: &mut WorkflowRouter<B>) -> Result<(), BoxDynError> {
        let svc = SteppedService::new(DelayWithStep {
            f: self.f.clone(),
            inner: self.inner.clone(),
            _marker: std::marker::PhantomData,
        });
        let count = ctx.steps.len();
        ctx.steps.insert(count, svc);
        self.inner.register(ctx)
    }
}

impl<S, F, B: BackendExt + Send + Sync + 'static + Clone, Input, Err>
    Service<Task<B::Compact, B::Context, B::IdType>> for DelayWithStep<S, F, B, Input>
where
    F: FnMut(Task<Input, B::Context, B::IdType>) -> Duration + Send + 'static + Clone,
    S: Step<Input, B> + Send + 'static,
    S::Response: Send + 'static,
    B::IdType: GenerateId + Send + 'static,
    B::Compact: Send + 'static,
    B: Sink<Task<B::Compact, B::Context, B::IdType>, Error = Err> + Unpin + Send + Sync,
    Err: std::error::Error + Send + Sync + 'static,
    B::Codec: Codec<Duration, Compact = B::Compact> + Codec<Input, Compact = B::Compact> + 'static,
    <B::Codec as Codec<Duration>>::Error: Into<BoxDynError>,
    <B::Codec as Codec<Input>>::Error: Into<BoxDynError>,
    B::Context: Send + 'static + MetadataExt<WorkflowContext>,
{
    type Response = GoTo<StepResult<B::Compact, B::IdType>>;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Task<B::Compact, B::Context, B::IdType>) -> Self::Future {
        let mut ctx: StepContext<B> = req.parts.data.get().cloned().unwrap();
        let mut f = self.f.clone();

        let task_id = TaskId::new(B::IdType::generate());
        async move {
            let decoded: Input = B::Codec::decode(&req.args)
                .map_err(|e: <B::Codec as Codec<Input>>::Error| e.into())?;
            let (args, parts) = req.take();
            let delay_duration = f(Task {
                args: decoded,
                parts,
            });
            let task = TaskBuilder::new(args)
                .with_task_id(task_id.clone())
                .meta(WorkflowContext {
                    step_index: ctx.current_step + 1,
                })
                .run_after(delay_duration)
                .build();
            ctx.backend
                .send(task)
                .await
                .map_err(|e| BoxDynError::from(e))?;
            Ok(GoTo::Next(StepResult {
                result: B::Codec::encode(&delay_duration).map_err(|e| e.into())?,
                next_task_id: Some(task_id),
            }))
        }
        .boxed()
    }
}

impl<Start, Cur, B, L> Workflow<Start, Cur, B, L> {
    /// Delay the workflow by a fixed duration
    pub fn delay(self, delay: Duration) -> Workflow<Start, Cur, B, Stack<DelayFor, L>> {
        self.add_step(DelayFor { duration: delay })
    }
}
impl<Start, Cur, B, L> Workflow<Start, Cur, B, L> {
    /// Delay the workflow by a duration determined by a function
    pub fn delay_with<F, I>(self, f: F) -> Workflow<Start, I, B, Stack<DelayWith<F, B, I>, L>> {
        self.add_step(DelayWith {
            f,
            _marker: std::marker::PhantomData,
        })
    }
}
