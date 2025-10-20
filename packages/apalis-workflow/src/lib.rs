#![doc = include_str!("../README.md")]
use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    task::{Context, Poll},
    time::Duration,
};

use apalis_core::{
    backend::{Backend, TaskSink, TaskSinkError, WeakTaskSink, codec::Codec},
    error::BoxDynError,
    task::{Task, builder::TaskBuilder, metadata::MetadataExt, task_id::TaskId},
    worker::builder::IntoWorkerService,
};
use futures::{Sink, future::BoxFuture};
use serde::{Deserialize, Serialize};
use tower::Service;

use crate::{context::StepContext, service::WorkFlowService};

mod context;
mod id_generator;
mod service;
mod steps;

pub use crate::steps::{delay::DelayStep, filter_map::FilterMapStep, then::ThenStep};
pub use id_generator::GenerateId;
pub use service::{StepResult, handle_workflow_result};

type BoxedService<Input, Output> = tower::util::BoxService<Input, Output, BoxDynError>;
type SteppedService<Compact, Ctx, IdType> = BoxedService<Task<Compact, Ctx, IdType>, Compact>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GoTo<T = ()> {
    Next(T),
    DelayFor(Duration, T),
    Done,
    /// Breaks the current task execution
    Break(T),

    /// Execution will continue in another task identified by the String
    /// Returning this does not guarantee that the task will be executed.
    /// It may be an invalid task id, or the task may never be scheduled.
    ContinueAt(String),
}

pub trait Step<Args, FlowSink, Encode>
where
    FlowSink: WeakTaskSink<Self::Response>,
{
    type Response;
    type Error: Send;
    fn run(
        &mut self,
        ctx: &StepContext<FlowSink, Encode>,
        step: Task<Args, FlowSink::Context, FlowSink::IdType>,
    ) -> impl Future<Output = Result<GoTo<Self::Response>, Self::Error>> + Send;
}

pub struct WorkFlow<Input, Current, FlowSink, Encode, Compact, Context, IdType> {
    name: String,
    steps: HashMap<usize, CompositeService<FlowSink, Encode, Compact, Context, IdType>>,
    _marker: PhantomData<(Input, Current, FlowSink)>,
}

pub struct CompositeService<FlowSink, Encode, Compact, Context, IdType> {
    svc: SteppedService<Compact, Context, IdType>,
    _marker: PhantomData<(FlowSink, Encode)>,
}

impl<Input, FlowSink, Encode, Compact, Context, IdType>
    WorkFlow<Input, Input, FlowSink, Encode, Compact, Context, IdType>
{
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            steps: HashMap::new(),
            _marker: PhantomData,
        }
    }
}

impl<Input, Current, FlowSink, Encode, Compact>
    WorkFlow<Input, Current, FlowSink, Encode, Compact, FlowSink::Context, FlowSink::IdType>
where
    Current: Send + 'static,
    FlowSink: Send + Clone + Sync + 'static + Unpin + Backend,
{
    pub fn add_step<S, Res, E, CodecError, BackendError>(
        mut self,
        step: S,
    ) -> WorkFlow<Input, Res, FlowSink, Encode, Compact, FlowSink::Context, FlowSink::IdType>
    where
        FlowSink: WeakTaskSink<Res, Codec = Encode, Error = BackendError>
            + Sink<Task<Compact, FlowSink::Context, FlowSink::IdType>, Error = BackendError>,
        Current: std::marker::Send + 'static + Sync,
        FlowSink::Context: Send + 'static + Sync,
        S: Step<Current, FlowSink, Encode, Response = Res, Error = E>
            + Sync
            + Send
            + 'static
            + Clone,
        S::Response: Send,
        S::Error: Send,
        Res: 'static + Sync,
        FlowSink::IdType: Send,
        Encode: Codec<Current, Compact = Compact, Error = CodecError>
            + Codec<GoTo<Res>, Compact = Compact, Error = CodecError>
            + Codec<Res, Compact = Compact, Error = CodecError>,
        Compact: Send + Sync + 'static,
        Encode: Send + Sync + 'static,
        E: Into<BoxDynError> + Send + Sync + 'static,
        CodecError: std::error::Error + Send + 'static + Sync,
        FlowSink::Context: MetadataExt<WorkflowRequest>,
        BackendError: std::error::Error + Send + Sync + 'static,
    {
        self.steps.insert(self.steps.len(), {
            let svc =
                SteppedService::<Compact, FlowSink::Context, FlowSink::IdType>::new(StepService {
                    codec: PhantomData::<(Encode, Current, FlowSink)>,
                    step,
                });
            CompositeService {
                svc,
                _marker: PhantomData,
            }
        });
        WorkFlow {
            name: self.name,
            steps: self.steps,
            _marker: PhantomData,
        }
    }
}

pub struct StepService<Step, Encode, Args, FlowSink> {
    step: Step,
    codec: PhantomData<(Encode, Args, FlowSink)>,
}

impl<Args, S, Encode, Compact, FlowSink, E, CodecError, BackendErr>
    Service<Task<Compact, FlowSink::Context, FlowSink::IdType>>
    for StepService<S, Encode, Args, FlowSink>
where
    S: Step<Args, FlowSink, Encode, Error = E> + Clone + Send + 'static,
    Encode: Codec<Args, Compact = Compact, Error = CodecError>
        + Codec<S::Response, Compact = Compact, Error = CodecError>
        + Codec<GoTo<S::Response>, Compact = Compact, Error = CodecError>,
    S::Response: Send + 'static + Sync,
    S::Error: Send + 'static,
    FlowSink: Clone
        + Send
        + 'static
        + Sync
        + WeakTaskSink<S::Response, Codec = Encode, Error = BackendErr>
        + Unpin
        + Sink<Task<Compact, FlowSink::Context, FlowSink::IdType>, Error = BackendErr>,
    Args: Send + 'static,
    FlowSink::Context: Send + 'static + MetadataExt<WorkflowRequest>,
    FlowSink::IdType: Send + 'static,
    Compact: Send + Sync + 'static,
    Encode: Send + Sync + 'static,
    E: Into<BoxDynError> + Send + 'static + Sync,
    CodecError: std::error::Error + Send + 'static + Sync,
    BackendErr: std::error::Error + Send + 'static + Sync,
{
    type Response = Compact;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: Task<Compact, FlowSink::Context, FlowSink::IdType>) -> Self::Future {
        let ctx: Option<StepContext<FlowSink, Encode>> = req.parts.data.get().cloned();
        let mut step = self.step.clone();
        Box::pin(async move {
            match ctx {
                Some(ctx) => {
                    let mut ctx = ctx.clone();
                    let req = req.try_map(|arg| Encode::decode(&arg));
                    match req {
                        Ok(task) => {
                            let res = step.run(&ctx, task).await.map_err(|e| e.into())?;

                            let _ = handle_workflow_result::<
                                S::Response,
                                Compact,
                                FlowSink,
                                BackendErr,
                            >(&mut ctx, &res)
                            .await
                            .map_err(|e| match e {
                                TaskSinkError::PushError(err) => Box::new(err) as BoxDynError,
                                TaskSinkError::CodecError(err) => {
                                    WorkflowError::CodecError(err.into()).into()
                                }
                            })?;
                            Encode::encode(&res)
                                .map_err(|e| WorkflowError::CodecError(e.into()).into())
                        }
                        Err(e) => Err(WorkflowError::CodecError(e.into()).into()),
                    }
                }
                None => Err(WorkflowError::MissingContextError.into()),
            }
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WorkflowError {
    #[error("Missing StepContext")]
    MissingContextError,
    #[error("CodecError: {0}")]
    CodecError(BoxDynError),
    #[error("SingleStepError: {0}")]
    SingleStepError(BoxDynError),
    #[error("SinkError: {0}")]
    SinkError(BoxDynError),
    #[error("MetadataError: {0}")]
    MetadataError(BoxDynError),
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct WorkflowRequest {
    pub step_index: usize,
}

impl<Input, Current, FlowSink, Encode, Compact, Err>
    IntoWorkerService<
        FlowSink,
        WorkFlowService<FlowSink, Encode, Compact, FlowSink::Context, FlowSink::IdType>,
        Compact,
        FlowSink::Context,
    > for WorkFlow<Input, Current, FlowSink, Encode, Compact, FlowSink::Context, FlowSink::IdType>
where
    FlowSink: Clone
        + Send
        + Sync
        + 'static
        + Sink<Task<Compact, FlowSink::Context, FlowSink::IdType>, Error = Err>
        + Unpin,
    Err: std::error::Error + Send + Sync + 'static,
    Compact: Send,
    FlowSink: TaskSink<Compact, Codec = Encode>,
    FlowSink::Context: MetadataExt<WorkflowRequest> + Send + Sync + 'static,
    Encode: Send + Sync + 'static + Codec<Compact, Compact = Compact>,
    Compact: Send + Sync + 'static + Clone,
    FlowSink::IdType: Send + 'static + Default,
    FlowSink: Sync + Backend<Args = Compact, Error = Err>,
    Compact: Send + Sync,
    FlowSink::Context: Send + Default + MetadataExt<WorkflowRequest>,
    FlowSink::IdType: GenerateId,
    <FlowSink::Context as MetadataExt<WorkflowRequest>>::Error: Into<BoxDynError>,
    Encode::Error: Into<BoxDynError>,
{
    fn into_service(
        self,
        b: &FlowSink,
    ) -> WorkFlowService<FlowSink, Encode, Compact, FlowSink::Context, FlowSink::IdType> {
        let services: HashMap<usize, _> = self
            .steps
            .into_iter()
            .map(|(index, svc)| (index, svc))
            .collect();
        WorkFlowService::new(services, b.clone())
    }
}

pub trait TaskFlowSink<Args, Compact>: Backend
where
    Self::Codec: Codec<Args>,
{
    fn push_start(&mut self, step: Args) -> impl Future<Output = Result<(), WorkflowError>> + Send {
        self.push_step(step, 0)
    }

    fn push_step(
        &mut self,
        step: Args,
        index: usize,
    ) -> impl Future<Output = Result<(), WorkflowError>> + Send;
}

impl<S: Send, Args: Send, Compact> TaskFlowSink<Args, Compact> for S
where
    S: WeakTaskSink<Args>,
    S::IdType: Default + Send,
    S::Codec: Codec<Args, Compact = Compact>,
    S::Context: MetadataExt<WorkflowRequest> + Send,
    S::Error: std::error::Error + Send + Sync + 'static,
    <S::Codec as Codec<Args>>::Error: Into<BoxDynError> + Send + Sync + 'static,
    <S::Context as MetadataExt<WorkflowRequest>>::Error: Into<BoxDynError> + Send + Sync + 'static,
{
    async fn push_step(&mut self, step: Args, index: usize) -> Result<(), WorkflowError> {
        let task_id = TaskId::new(S::IdType::default());
        let task = TaskBuilder::new(step)
            .meta(WorkflowRequest { step_index: index })
            .with_task_id(task_id.clone())
            .build();
        self.push_task(task)
            .await
            .map_err(|e| WorkflowError::SinkError(e.into()))
    }
}

#[cfg(test)]
mod tests {

    use apalis_core::{
        backend::json::JsonStorage,
        worker::{builder::WorkerBuilder, event::Event, ext::event_listener::EventListenerExt},
    };

    use std::time::Duration;

    use crate::{TaskFlowSink, WorkFlow, WorkflowError};

    #[tokio::test]
    async fn simple_workflow() {
        let workflow = WorkFlow::new("odd-numbers-workflow")
            .then(|a: usize| async move { Ok::<_, WorkflowError>(a - 2) })
            .delay_for(Duration::from_millis(1000))
            .then(|_| async move { Err::<(), WorkflowError>(WorkflowError::MissingContextError) });

        let mut in_memory = JsonStorage::new_temp().unwrap();

        in_memory.push_start(usize::MAX).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .on_event(|ctx, ev| {
                println!("On Event = {:?}", ev);
                if matches!(ev, Event::Error(_)) {
                    ctx.stop().unwrap();
                }
            })
            .build(workflow);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn then_workflow() {
        let workflow = WorkFlow::new("then-workflow")
            .then(|a: usize| async move { Ok::<_, WorkflowError>((0..a).collect::<Vec<_>>()) })
            .filter_map(|x| async move { if x % 5 != 0 { Some(x) } else { None } })
            .filter_map(|x| async move { if x % 3 != 0 { Some(x) } else { None } })
            .filter_map(|x| async move { if x % 2 != 0 { Some(x) } else { None } })
            .then(|a| async move {
                dbg!(a);
                Err::<(), WorkflowError>(WorkflowError::MissingContextError)
            });

        let mut in_memory = JsonStorage::new_temp().unwrap();

        in_memory.push_start(100).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .on_event(|ctx, ev| {
                println!("On Event = {:?}", ev);
                if matches!(ev, Event::Error(_)) {
                    ctx.stop().unwrap();
                }
            })
            .build(workflow);
        worker.run().await.unwrap();
    }
}
