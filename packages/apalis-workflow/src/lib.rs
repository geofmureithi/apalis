use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    sync::Arc,
    task::{Context, Poll},
};

use apalis_core::{
    backend::{codec::Codec, Backend, BackendWithCodec, TaskSink},
    error::BoxDynError,
    task::{builder::TaskBuilder, metadata::MetadataExt, task_id::TaskId, Task},
    worker::builder::WorkerServiceBuilder,
};
use futures::{
    future::{ready, BoxFuture},
    FutureExt, TryFutureExt,
};
use serde::{Deserialize, Serialize};
use tower::Service;

use crate::{context::StepContext, service::WorkFlowService};

mod context;
mod service;
mod steps;

type BoxedService<Input, Output> = tower::util::BoxService<Input, Output, BoxDynError>;
type SteppedService<Compact, Meta, IdType> =
    BoxedService<Task<Compact, Meta, IdType>, (bool, Compact)>;

pub trait Step<Args, FlowSink, Encode>
where
    Encode: Codec<Args>,
    FlowSink: TaskSink<Encode::Compact>,
{
    type Response;
    type Error: Send;
    fn pre(
        ctx: &mut StepContext<FlowSink, Encode>,
        step: &Args,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        ready(Ok(()))
    }

    fn run(
        &mut self,
        ctx: &StepContext<FlowSink, Encode>,
        step: Task<Args, FlowSink::Meta, FlowSink::IdType>,
    ) -> impl Future<Output = Result<Self::Response, Self::Error>> + Send;

    fn post(
        &self,
        ctx: &StepContext<FlowSink, Encode>,
        res: &Self::Response,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send {
        ready(Ok(true)) // By default run the next hook
    }
}

pub struct WorkFlow<Input, Current, FlowSink, Encode, Compact>
where
    FlowSink: TaskSink<Compact>,
{
    name: String,
    steps: HashMap<usize, CompositeService<FlowSink, Encode, Compact>>,
    _marker: PhantomData<(Input, Current, FlowSink)>,
}

pub struct CompositeService<FlowSink, Encode, Compact>
where
    FlowSink: TaskSink<Compact>,
{
    pre_hook: Arc<
        Box<
            dyn Fn(
                    &StepContext<FlowSink, Encode>,
                    &Compact,
                ) -> BoxFuture<'static, Result<(), BoxDynError>>
                + Send
                + Sync
                + 'static,
        >,
    >,
    svc: SteppedService<Compact, FlowSink::Meta, FlowSink::IdType>,
}

impl<Input, FlowSink, Encode, Compact> WorkFlow<Input, Input, FlowSink, Encode, Compact>
where
    FlowSink: TaskSink<Compact>,
{
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            steps: HashMap::new(),
            _marker: PhantomData,
        }
    }
}

impl<Input, Current, FlowSink, Encode, Compact> WorkFlow<Input, Current, FlowSink, Encode, Compact>
where
    Current: Send + 'static,
    FlowSink: Send + Clone + Sync + 'static + Unpin + TaskSink<Compact>,
{
    pub fn add_step<S, Res, E, CodecError>(
        mut self,
        step: S,
    ) -> WorkFlow<Input, Res, FlowSink, Encode, Compact>
    where
        Current: std::marker::Send + 'static + Sync,
        FlowSink::Meta: Send + 'static + Sync,
        S: Step<Current, FlowSink, Encode, Response = Res, Error = E>
            + Sync
            + Send
            + 'static
            + Clone,
        S::Response: Send,
        S::Error: Send,
        Res: 'static,
        FlowSink::IdType: Send,
        Encode: Codec<Current, Compact = Compact, Error = CodecError>
            + Codec<Res, Compact = Compact, Error = CodecError>,
        Compact: Send + Sync + 'static + Clone,
        Encode: Send + Sync + 'static,
        E: Into<BoxDynError> + Send + Sync + 'static,
        CodecError: std::error::Error + Send + 'static + Sync,
    {
        self.steps.insert(self.steps.len(), {
            let pre_hook = Arc::new(Box::new(
                move |ctx: &StepContext<FlowSink, Encode>, step: &Compact| {
                    let val = Encode::decode(step.clone());
                    match val {
                        Ok(val) => {
                            let mut ctx = ctx.clone();
                            async move {
                                S::pre(&mut ctx, &val).await.map_err(|e| e.into())?;
                                Ok(())
                            }
                            .boxed()
                        }
                        Err(e) => ready(Err(e.into())).boxed(),
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
                SteppedService::<Compact, FlowSink::Meta, FlowSink::IdType>::new(StepService {
                    codec: PhantomData::<(Encode, Current, FlowSink)>,
                    step,
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

pub struct StepService<Step, Encode, Args, FlowSink> {
    step: Step,
    codec: PhantomData<(Encode, Args, FlowSink)>,
}

impl<Args, S, Encode, Compact, FlowSink, E, CodecError>
    Service<Task<Compact, FlowSink::Meta, FlowSink::IdType>>
    for StepService<S, Encode, Args, FlowSink>
where
    S: Step<Args, FlowSink, Encode, Error = E> + Clone + Send + 'static,
    Encode: Codec<Args, Compact = Compact, Error = CodecError>
        + Codec<S::Response, Compact = Compact, Error = CodecError>,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
    FlowSink: Clone + Send + 'static + Sync + TaskSink<Compact>,
    Args: Send + 'static,
    FlowSink::Meta: Send + 'static,
    FlowSink::IdType: Send + 'static,
    Compact: Send + Sync + 'static,
    Encode: Send + Sync + 'static,
    E: Into<BoxDynError> + Send + 'static + Sync,
    CodecError: std::error::Error + Send + 'static + Sync,
{
    type Response = (bool, Compact);
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: Task<Compact, FlowSink::Meta, FlowSink::IdType>) -> Self::Future {
        let ctx: Option<StepContext<FlowSink, Encode>> = req.get().cloned();
        match ctx {
            Some(ctx) => {
                let req = req.try_map(|arg| Encode::decode(arg));

                match req {
                    Ok(task) => {
                        let mut step = self.step.clone();
                        Box::pin(async move {
                            let res = step
                                .run(&ctx, task)
                                .await
                                .map_err(|e| WorkflowError::SingleStepError(e.into()))?;
                            let should_next = step
                                .post(&ctx, &res)
                                .await
                                .map_err(|e| WorkflowError::SingleStepError(e.into()))?;
                            Ok((
                                should_next,
                                Encode::encode(&res)
                                    .map_err(|e| WorkflowError::CodecError(e.into()))?,
                            ))
                        })
                    }
                    Err(e) => ready(Err(WorkflowError::CodecError(e.into()))).boxed(),
                }
            }
            None => ready(Err(WorkflowError::MissingContextError)).boxed(),
        }
        .map_err(|e| BoxDynError::from(e))
        .boxed()
        // Todo: Remove lots of boxes
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

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct WorkflowRequest {
    pub step_index: usize,
}

impl<Input, Current, FlowSink, Encode, Compact>
    WorkerServiceBuilder<
        FlowSink,
        WorkFlowService<FlowSink, Encode, Compact>,
        Compact,
        FlowSink::Meta,
    > for WorkFlow<Input, Current, FlowSink, Encode, Compact>
where
    FlowSink: Clone,
    Compact: Send,
    FlowSink: TaskSink<Compact> + BackendWithCodec<Codec = Encode, Compact = Compact>,
{
    fn build(self, b: &FlowSink) -> WorkFlowService<FlowSink, Encode, Compact> {
        let services: HashMap<usize, _> = self
            .steps
            .into_iter()
            .map(|(index, svc)| (index, svc))
            .collect();
        WorkFlowService::new(services, b.clone())
    }
}

pub trait TaskFlowSink<Args>: BackendWithCodec + Backend<Self::Compact> {
    fn push_start(&mut self, step: Args) -> impl Future<Output = Result<(), WorkflowError>> + Send {
        self.push_step(step, 0)
    }

    fn push_step(
        &mut self,
        step: Args,
        index: usize,
    ) -> impl Future<Output = Result<(), WorkflowError>> + Send;
}

impl<S: Send, Args: Send> TaskFlowSink<Args> for S
where
    S: TaskSink<Self::Compact> + BackendWithCodec + Backend<Self::Compact>,
    S::IdType: Default + Clone + Send,
    <S as BackendWithCodec>::Codec: Codec<Args, Compact = Self::Compact>,
    S::Meta: MetadataExt<WorkflowRequest> + Send,
    S::Error: Into<BoxDynError> + Send + Sync + 'static,
    <<S as BackendWithCodec>::Codec as Codec<Args>>::Error:
        Into<BoxDynError> + Send + Sync + 'static,
    <S::Meta as MetadataExt<WorkflowRequest>>::Error: Into<BoxDynError> + Send + Sync + 'static,
{
    async fn push_step(&mut self, step: Args, index: usize) -> Result<(), WorkflowError> {
        let task_id = TaskId::new(S::IdType::default());
        let mut meta = S::Meta::default();
        meta.inject(WorkflowRequest { step_index: index })
            .map_err(|e| WorkflowError::MetadataError(e.into()))?;
        let task = TaskBuilder::new_with_metadata(
            S::Codec::encode(&step).map_err(|e| WorkflowError::CodecError(e.into()))?,
            meta,
        )
        .with_task_id(task_id.clone())
        .build();
        self.push_raw(task)
            .await
            .map_err(|e| WorkflowError::SinkError(e.into()))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, convert::Infallible};

    use apalis_core::{
        backend::{memory::MemoryStorage, TaskSink},
        error::BoxDynError,
        service_fn::service_fn,
        task::{status::Status, Task},
        worker::{
            builder::WorkerBuilder, context::WorkerContext, event::Event,
            ext::event_listener::EventListenerExt,
        },
    };
    use serde_json::{Number, Value};
    use tower::{steer::Steer, util::BoxService, ServiceExt};

    use super::*;

    const ITEMS: u32 = 10;

    #[tokio::test]
    async fn it_works() {
        let workflow = WorkFlow::new("count_to_100")
            .then(|a: usize| async move { Ok::<_, WorkflowError>(vec![a, a + 1]) })
            .then(|res, wrk: WorkerContext| async move {
                dbg!(res);
                wrk.stop().unwrap();
            })
            .then(|_| async {
                unreachable!("Worker should have stopped");
            });

        let mut in_memory = MemoryStorage::new_with_json();

        in_memory.push_step(usize::MIN, 0).await.unwrap();

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
