use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    sync::Arc,
    task::{Context, Poll},
};

use apalis_core::{
    backend::{codec::Codec, BackendWithCodec, TaskSink},
    error::BoxDynError,
    task::Task,
    worker::builder::WorkerServiceBuilder,
};
use futures::{
    future::{ready, BoxFuture},
    FutureExt,
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
    pub fn add_step<S, Res>(mut self, step: S) -> WorkFlow<Input, Res, FlowSink, Encode, Compact>
    where
        Current: std::marker::Send + 'static + Sync,
        FlowSink::Meta: Send + 'static + Sync,
        S: Step<Current, FlowSink, Encode, Response = Res, Error = BoxDynError>
            + Clone
            + Sync
            + Send
            + 'static,
        S::Response: Send,
        S::Error: Debug + Send,
        Res: 'static,
        FlowSink::IdType: Send,
        FlowSink::Meta: Clone,
        Encode: Codec<Current, Compact = Compact> + Codec<Res, Compact = Compact>,
        Compact: Clone,
        <Encode as Codec<Current>>::Error: Debug,
        <Encode as Codec<Res>>::Error: Debug,
        Compact: Send + Sync + 'static,
        Encode: Send + Sync + 'static + Clone,
    {
        self.steps.insert(self.steps.len(), {
            let pre_hook = Arc::new(Box::new(
                move |ctx: &StepContext<FlowSink, Encode>, step: &Compact| {
                    dbg!(ctx.current_step);
                    let val = Encode::decode(step.clone()).unwrap();
                    let mut ctx = ctx.clone();
                    async move {
                        S::pre(&mut ctx, &val).await?;
                        Ok(())
                    }
                    .boxed()
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

impl<Args, S, Encode, Compact, FlowSink> Service<Task<Compact, FlowSink::Meta, FlowSink::IdType>>
    for StepService<S, Encode, Args, FlowSink>
where
    S: Step<Args, FlowSink, Encode> + Clone + Send + 'static,
    Encode: Codec<Args, Compact = Compact> + Codec<S::Response, Compact = Compact>,
    S::Response: Send + 'static,
    <Encode as Codec<Args>>::Error: Debug,
    <Encode as Codec<S::Response>>::Error: Debug,
    S::Error: Debug + Send + 'static,
    FlowSink: Clone + Send + 'static + Sync + TaskSink<Compact>,
    Args: Send + 'static,
    FlowSink::Meta: Send + 'static,
    FlowSink::IdType: Send + 'static,
    Compact: Send + Sync + 'static + Clone,
    Encode: Send + Sync + Clone + 'static,
{
    type Response = (bool, Compact);
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, S::Error>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: Task<Compact, FlowSink::Meta, FlowSink::IdType>) -> Self::Future {
        let ctx: StepContext<FlowSink, Encode> = req.get().cloned().unwrap();
        dbg!(&ctx.current_step);
        let req = req.try_map(|arg| Encode::decode(arg)).unwrap();

        let mut step = self.step.clone();
        Box::pin(async move {
            let res = step.run(&ctx, req).await.unwrap();
            let should_next = step.post(&ctx, &res).await.unwrap();
            Ok((should_next, Encode::encode(&res).unwrap()))
        })
    }
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct WorkflowRequest {
    pub step_index: usize,
}

impl<Args, Input, Current, FlowSink, Encode, Compact>
    WorkerServiceBuilder<FlowSink, WorkFlowService<FlowSink, Encode, Compact>, Args, FlowSink::Meta>
    for WorkFlow<Input, Current, FlowSink, Encode, Compact>
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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, convert::Infallible};

    use apalis_core::{
        backend::{memory::MemoryStorage, TaskSink},
        error::BoxDynError,
        service_fn::service_fn,
        task::{status::Status, Task},
        worker::{
            builder::WorkerBuilder, context::WorkerContext, ext::event_listener::EventListenerExt,
        },
    };
    use serde_json::{Number, Value};
    use tower::{steer::Steer, util::BoxService, ServiceExt};

    use crate::WorkFlow;

    const ITEMS: u32 = 10;

    #[tokio::test]
    async fn it_works() {
        let workflow = WorkFlow::new("count_to_100")
            .then(|a: usize| async move { Ok::<_, BoxDynError>(vec![a, a + 1]) })
            .then(|res, wrk: WorkerContext| async move {
                dbg!(res);
                wrk.stop().unwrap();
            });

        let mut in_memory = MemoryStorage::new_with_json();

        TaskSink::push(&mut in_memory, Value::Number(Number::from_i128(1).unwrap()))
            .await
            .unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .on_event(|ctx, ev| {
                println!("On Event = {:?}", ev);
            })
            .build(workflow);
        worker.run().await.unwrap();
    }
}
