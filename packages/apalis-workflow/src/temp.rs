use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures::{
    future::{ready, BoxFuture},
    FutureExt, Sink, Stream, StreamExt,
};
// use futures::{channel::mpsc::Receiver, stream::BoxStream, StreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use tower::{
    steer::{Picker, Steer},
    util::BoxService,
    Service, ServiceBuilder, ServiceExt,
};

use apalis_core::{
    backend::{
        codec::{json::JsonCodec, Codec},
        memory::{JsonMemory, MemoryStorage},
        Backend, TaskResult, TaskSink, WaitForCompletion,
    },
    error::BoxDynError,
    service_fn::{service_fn, ServiceFn},
    task::{
        builder::TaskBuilder,
        metadata::MetadataExt,
        task_id::{RandomId, TaskId},
        ExecutionContext, Task,
    },
    worker::builder::WorkerServiceBuilder,
};

use crate::service::WorkFlowService;

pub mod service;
// use crate::{backend::Backend, error::BoxDynError, request::Request, worker::context::WorkerContext};

// pub mod dag;
// pub mod stepped;
// pub mod branch;
// pub mod sink;
// pub mod layer;

/// Allows control of the next flow
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum GoTo<N> {
    /// Go to the next flow immediately
    Next(N),
    /// Delay the next flow for some time
    Delay {
        /// The input of the next flow
        next: N,
        /// The period to delay
        delay: Duration,
    },
    /// Complete execution
    Done(N),

    Filter(bool),

    FilterMap(N),
}
pub trait Step<Args, FlowSink, Compact>
where
    FlowSink: TaskSink<Compact>,
{
    type Response;
    type Error: Send;
    fn pre(
        ctx: &mut StepContext<FlowSink, Compact>,
        step: &Args,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        ready(Ok(()))
    }

    fn run(
        &mut self,
        ctx: &StepContext<FlowSink, Compact>,
        step: Task<Args, FlowSink::Meta, FlowSink::IdType>,
    ) -> impl Future<Output = Result<Self::Response, Self::Error>> + Send;

    fn post(
        &self,
        ctx: &mut StepContext<FlowSink, Compact>,
        res: &Self::Response,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send {
        ready(Ok(true)) // By default run the next hook
    }
}

// pub type BoxStep<Args> = Box<dyn Step<Args, Response = Value, Error = BoxDynError>>;

pub struct StepService<S, C, Args, FlowSink> {
    step: S,
    codec: PhantomData<(C, Args, FlowSink)>,
}

impl<Args, S, C, Compact, B> Service<Task<Compact, B::Meta, B::IdType>>
    for StepService<S, C, Args, B>
where
    S: Step<Args, B, Compact> + Clone + Send + 'static,
    C: Codec<Args, Compact = Compact> + Codec<S::Response, Compact = Compact>,
    S::Response: Send + 'static,
    <C as Codec<Args>>::Error: Debug,
    <C as Codec<S::Response>>::Error: Debug,
    S::Error: Debug + Send + 'static,
    B: Clone + Send + 'static + Sync + TaskSink<Compact>,
    Args: Send + 'static,
    B::Meta: Send + 'static,
    B::IdType: Send + 'static,
    Compact: Send + Sync + 'static + Clone
{
    type Response = (bool, Compact);
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, S::Error>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: Task<Compact, B::Meta, B::IdType>) -> Self::Future {
        let mut ctx: StepContext<B, Compact> = req.get().cloned().unwrap();
        dbg!(&ctx.current_step);
        let req = req.try_map(|arg| C::decode(arg)).unwrap();

        let mut step = self.step.clone();
        Box::pin(async move {
            let res = step.run(&ctx, req).await.unwrap();
            let should_next = step.post(&mut ctx, &res).await.unwrap();
            Ok((should_next, C::encode(&res).unwrap()))
        })
    }
}

pub struct FilterService<S, C, Args, Meta, B, O, F, FnArgs> {
    step: S,
    codec: PhantomData<(C, Args, Meta, O, F, FnArgs, B)>,
}

impl<Args, S, C, Compact, B, O, F: 'static, FnArgs: 'static, E>
    Service<Task<Compact, B::Meta, B::IdType>>
    for FilterService<S, C, Args, B::Meta, B, O, F, FnArgs>
where
    S: Step<Args, B, Compact, Response = Option<O>> + Clone + Send + Sync + 'static,
    C: Codec<Args, Compact = Compact>
        + Codec<Option<O>, Compact = Compact>
        + Codec<Vec<Args>, Compact = Compact>
        + Codec<Vec<O>, Compact = Compact>,
    O: Send + Sync + 'static,
    <C as Codec<Args>>::Error: Debug,
    <C as Codec<S::Response>>::Error: Debug,
    S::Error: Debug + Send + 'static,
    B: Clone + Send + 'static + Sync,
    Args: Send + 'static + Sync + Serialize + DeserializeOwned,
    B::Meta:
        Send + 'static + MetadataExt<FilterContext> + MetadataExt<WorkflowRequest> + Sync + Default,
    B: TaskSink<Compact> + Unpin,
    ServiceFn<F, Args, B::Meta, FnArgs>:
        Service<Task<Args, B::Meta, B::IdType>, Response = Option<O>, Error = E> + Send + Sync,
    E: Into<BoxDynError> + Send + Sync + 'static,
    <B::Meta as MetadataExt<WorkflowRequest>>::Error: Debug,
    <B::Meta as MetadataExt<FilterContext>>::Error: Debug,
    <ServiceFn<F, Args, B::Meta, FnArgs> as Service<Task<Args, B::Meta, B::IdType>>>::Future:
        Send + 'static,
    B::Error: Debug,
    <C as Codec<Vec<O>>>::Error: Debug,
    <C as Codec<Vec<Args>>>::Error: Debug,
    Compact: Send + Sync + Serialize + Clone + 'static,
{
    type Response = (bool, Compact);
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, S::Error>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: Task<Compact, B::Meta, B::IdType>) -> Self::Future {
        let mut ctx: StepContext<B, Compact> = req.get().cloned().unwrap();
        let filter_ctx: Result<FilterContext, _> = req.ctx.metadata.extract();
        match filter_ctx {
            Ok(_) => {
                dbg!(&ctx.current_step);
                let req = req.try_map(|arg| C::decode(arg)).unwrap();

                let mut step = FilterMap {
                    mapper: PhantomData::<
                        FilterMapStep<ServiceFn<F, Args, B::Meta, FnArgs>, Args, O>,
                    >,
                };
                Box::pin(async move {
                    let res = step.run(&ctx, req).await.unwrap();
                    let should_next = step.post(&mut ctx, &res).await.unwrap();
                    Ok((should_next, C::encode(&res).unwrap()))
                })
            }
            Err(_) => {
                dbg!(&ctx.current_step);
                let req = req.try_map(|arg| C::decode(arg)).unwrap();

                let mut step = self.step.clone();
                Box::pin(async move {
                    let res = step.run(&ctx, req).await.unwrap();
                    let should_next = step.post(&mut ctx, &res).await.unwrap();
                    Ok((should_next, C::encode(&res).unwrap()))
                })
            }
        }
    }
}

// pub struct ThenStep<F> {
//     f: F,
// }

// #[derive(Clone)]

// pub struct Then<S, F> {
//     inner: S,
//     f: F,
// }

// impl<S, F: Clone> Step<S> for ThenStep<F> {
//     type Service = Then<S, F>;
//     fn step(&self, inner: S) -> Self::Service {
//         Then {
//             f: self.f.clone(),
//             inner,
//         }
//     }
// }

// impl<Compact, Meta, S> Step<S> for StepBuilder<Compact, Meta>  {
//     type Service = ;
//     fn step(&self, inner: S) -> Self::Service {
//         self.steps.
//     }
// }

// pub trait StepExt {
//     fn then(&mut self, )
// }

type BoxedService<Input, Output> = tower::util::BoxService<Input, Output, BoxDynError>;
type SteppedService<Compact, Meta, IdType = RandomId> =
    BoxedService<Task<Compact, Meta, IdType>, (bool, Compact)>;

pub struct CompositeService<Compact, FlowSink>
where
    FlowSink: TaskSink<Compact>,
{
    pre_hook: Arc<
        Box<
            dyn Fn(&StepContext<FlowSink, Compact>, &Compact) -> BoxFuture<'static, Result<(), BoxDynError>>
                + Send
                + Sync
                + 'static,
        >,
    >,
    svc: SteppedService<Compact, FlowSink::Meta, FlowSink::IdType>,
}

pub struct WorkFlow<Input, Current, FlowSink, Compact>
where
    FlowSink: TaskSink<Compact>,
{
    steps: HashMap<usize, CompositeService<Compact, FlowSink>>,
    _marker: PhantomData<(Input, Current, FlowSink)>,
}

impl<Input, FlowSink, Compact> WorkFlow<Input, Input, FlowSink, Compact>
where
    FlowSink: TaskSink<Compact>,
{
    pub fn new(name: &str) -> Self {
        Self {
            steps: HashMap::new(),
            _marker: PhantomData,
        }
    }
}

impl<Input, Current, FlowSink, Compact> WorkFlow<Input, Current, FlowSink, Compact>
where
    Current: DeserializeOwned + Send + 'static,
    FlowSink: Send + Clone + Sync + 'static + Unpin + TaskSink<Compact>,
{
    pub fn then<F, O, E, FnArgs>(
        mut self,
        then: F,
    ) -> WorkFlow<Input, O, FlowSink, Compact>
    where
        O: Serialize + DeserializeOwned + Sync + Send + 'static,
        E: Into<BoxDynError> + Send + Sync + 'static,
        F: Send + 'static + Sync + Clone,
        ServiceFn<F, Current, FlowSink::Meta, FnArgs>:
            Service<Task<Current, FlowSink::Meta>, Response = O, Error = E>,
        FnArgs: std::marker::Send + 'static + Sync,
        Current: std::marker::Send + 'static + Serialize + Sync + Debug,
        FlowSink::Meta: Send + Sync + Default + 'static + MetadataExt<WorkflowRequest>,
    // Meta::Error: Debug,
        FlowSink::Error: Debug,
        <ServiceFn<F, Current, FlowSink::Meta, FnArgs> as Service<Task<Current, FlowSink::Meta>>>::Future:
            Send + 'static,
        <ServiceFn<F, Current, FlowSink::Meta, FnArgs> as Service<Task<Current, FlowSink::Meta>>>::Error:
            Into<BoxDynError>,
    {
        self.add_step(ThenStep {
            inner: service_fn::<F, Current, FlowSink::Meta, FnArgs>(then),
            _marker: PhantomData,
        })
    }

    pub fn add_step<O, S, Res>(mut self, step: S) -> WorkFlow<Input, O, FlowSink, Compact>
    where
        O: Send + 'static,
        Current: std::marker::Send + 'static + Serialize + Sync,
        FlowSink::Meta: Send + 'static + Sync,
        S: Step<Current, FlowSink, Compact, Response = Res, Error = BoxDynError>
            + Clone
            + Sync
            + Send
            + 'static,
        S::Response: Send,
        S::Error: Debug + Send,
        Res: Serialize + DeserializeOwned + 'static,
        FlowSink::IdType: Send,
        FlowSink::Meta: Clone,
    {
        self.steps.insert(self.steps.len(), {
            let pre_hook = Arc::new(Box::new(move |ctx: &StepContext<FlowSink, Compact>, step: &Compact| {
                dbg!(ctx.current_step);
                let val = JsonCodec::<Compact>::decode(step.clone()).unwrap();
                let mut ctx = ctx.clone();
                async move {
                    S::pre(&mut ctx, &val).await?;
                    Ok(())
                }
                .boxed()
            })
                as Box<
                    dyn Fn(
                            &StepContext<FlowSink, Compact>,
                            &Compact,
                        ) -> BoxFuture<'static, Result<(), BoxDynError>>
                        + Send
                        + Sync
                        + 'static,
                >);
            let svc = SteppedService::<Compact, FlowSink::Meta>::new(StepService {
                codec: PhantomData::<(JsonCodec<Compact>, Current, FlowSink::Meta)>,
                step,
            });
            CompositeService { pre_hook, svc }
        });
        WorkFlow {
            steps: self.steps,
            _marker: PhantomData,
        }
    }
}

impl<Input, Current, FlowSink, Compact> WorkFlow<Input, Vec<Current>, FlowSink, Compact>
where
    Current: DeserializeOwned + Send + 'static,
    FlowSink: TaskSink<Compact>,
    FlowSink::Meta: MetadataExt<FilterContext> + Send + 'static,
{
    pub fn filter_map<F, T, FnArgs, E>(
        mut self,
        predicate: F,
    ) -> WorkFlow<Input, Vec<T>, FlowSink, Compact>
    where
        F: Send + 'static + Sync,
        ServiceFn<F, Current, FlowSink::Meta, FnArgs>:
            Service<Task<Current, FlowSink::Meta, FlowSink::IdType>, Response = Option<T>, Error = E> + Clone,
        FnArgs: std::marker::Send + 'static,
        Current: std::marker::Send + 'static + Serialize + Sync + Debug,
        FlowSink::Meta: Send + 'static + Sync + Default + MetadataExt<WorkflowRequest>,
        <ServiceFn<F, Current, FlowSink::Meta, FnArgs> as Service<Task<Current, FlowSink::Meta>>>::Future:
            Send + 'static,
        <ServiceFn<F, Current, FlowSink::Meta, FnArgs> as Service<Task<Current, FlowSink::Meta>>>::Error:
            Into<BoxDynError>,
        T: Send + Serialize + 'static + Sync + DeserializeOwned,
        <FlowSink::Meta as MetadataExt<WorkflowRequest>>::Error: Debug,
        <FlowSink::Meta as MetadataExt<FilterContext>>::Error: Debug,
        FnArgs: Sync,
        FlowSink: Sync + Clone + Send + 'static + Unpin,
        FlowSink::Error: Debug,
        E: Send + Sync + 'static,
    {
        self.steps.insert(self.steps.len(), {
            let pre_hook = Arc::new(Box::new(move |ctx: &StepContext<FlowSink, Compact>, step: &Compact| {
                let val = JsonCodec::<Compact>::decode(step.clone());
                match val {
                    Ok(val) => {
                        let mut ctx = ctx.clone();
                        async move {
                            FilterMap::<ServiceFn<F, Current, FlowSink::Meta, FnArgs>, Current, T>::pre(
                                &mut ctx, &val,
                            )
                            .await?;
                            Ok(())
                        }
                        .boxed()
                    }
                    Err(_) => async move { Ok(()) }.boxed(),
                }
            })
                as Box<
                    dyn Fn(
                            &StepContext<FlowSink, Compact>,
                            &Compact,
                        ) -> BoxFuture<'static, Result<(), BoxDynError>>
                        + Send
                        + Sync
                        + 'static,
                >);

            let svc = SteppedService::<Compact, FlowSink::Meta>::new(FilterService {
                step: FilterMapStep {
                    _marker: PhantomData,
                    inner: service_fn(predicate),
                },
                codec: PhantomData::<(JsonCodec<Compact>, Current, FlowSink::Meta, T, F, FnArgs)>,
            });
            CompositeService { pre_hook, svc }
        });
        WorkFlow {
            steps: self.steps,
            _marker: PhantomData,
        }
    }

    // pub fn all<Fut, F>(mut self, next: F) -> WorkFlow<Input, Vec<Current>, serde_json::Value, Meta>
    // where
    //     F: FnMut(Current) -> Fut,
    //     Fut: Future<Output = bool>,
    // {
    //     // self.steps.insert(self.steps.len(), Box::new(next));
    //     WorkFlow {
    //         steps: self.steps,
    //         _marker: PhantomData,
    //     }
    // }

    // pub fn any<Fut, F>(mut self, next: F) -> WorkFlow<Input, bool, serde_json::Value, Meta>
    // where
    //     F: FnMut(Current) -> Fut,
    //     Fut: Future<Output = bool>,
    // {
    //     // self.steps.insert(self.steps.len(), Box::new(next));

    //     WorkFlow {
    //         steps: self.steps,
    //         _marker: PhantomData,
    //     }
    // }
}
impl<Input, Current, FlowSink, Compact> WorkFlow<Input, Current, FlowSink, Compact>
where
    FlowSink: TaskSink<Compact>,
{
    pub fn repeat<FnArgs, F, O, E>(
        mut self,
        repeat: usize,
        next: F,
    ) -> WorkFlow<Input, Vec<O>, FlowSink, Compact>
    where
        F: Send + 'static,
        ServiceFn<F, Current, FlowSink::Meta, FnArgs>: Service<Task<Current, FlowSink::Meta>, Response = O, Error = E>,
        FnArgs: std::marker::Send + 'static,
        Current: std::marker::Send + 'static,
    // Meta: Send + 'static,
        <ServiceFn<F, Current, FlowSink::Meta, FnArgs> as Service<Task<Current, FlowSink::Meta>>>::Future:
            Send + 'static,
        <ServiceFn<F, Current, FlowSink::Meta, FnArgs> as Service<Task<Current, FlowSink::Meta>>>::Error:
            Into<BoxDynError>,
    {
        // self.steps.insert(self.steps.len(), Box::new(next));
        WorkFlow {
            steps: self.steps,
            _marker: PhantomData,
        }
    }

    pub fn repeat_until<Fut, F, O, E>(
        mut self,
        next: F,
    ) -> WorkFlow<Input, Vec<O>, FlowSink, Compact>
    where
        F: FnMut(Current) -> Fut,
        Fut: Future<Output = Result<Option<O>, E>>,
    {
        // self.steps.insert(self.steps.len(), Box::new(next));
        WorkFlow {
            steps: self.steps,
            _marker: PhantomData,
        }
    }

    // pub fn chain<St>(self, other: St) -> Chain<Self, St> {
    //     // self.steps.insert(self.steps.len(), Box::new(next));
    //     self
    // }

    pub fn unzip<F>(mut self, next: F) -> Self {
        // self.steps.insert(self.steps.len(), Box::new(next));
        self
    }

    pub fn skip_while<F>(mut self, next: F) -> Self {
        // self.steps.insert(self.steps.len(), Box::new(next));
        self
    }

    pub fn steer<P: Picker<S, Current>, S>(mut self, steer: P) -> Self {
        // self.steps.insert(self.steps.len(), Box::new(next));
        self
    }
}

#[derive(Debug)]
pub struct ThenStep<S, T> {
    inner: S,
    _marker: std::marker::PhantomData<T>,
}

impl<S: Clone, T> Clone for ThenStep<S, T> {
    fn clone(&self) -> Self {
        ThenStep {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, T> ThenStep<S, T> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S, T, O, E, B, Compact> Step<T, B, Compact> for ThenStep<S, T>
where
    S: Service<Task<T, B::Meta, B::IdType>, Response = O, Error = E> + Sync + Send,
    T: DeserializeOwned + Sync + Serialize,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    E: Into<BoxDynError>,
    O: Sync,
    B: Sync + Unpin + TaskSink<Compact> + Send,
    T: Send + Debug + Clone,
    B::Meta: Send + Sync + Default + MetadataExt<WorkflowRequest>,
    B::Error: Debug + Send + 'static,
    // Meta::Error: Debug,
    B::IdType: Clone + Default + Send,
    Compact: Sync + Serialize + Clone + DeserializeOwned + Send,
{
    type Response = S::Response;
    type Error = BoxDynError;
    async fn pre(ctx: &mut StepContext<B, Compact>, step: &T) -> Result<(), Self::Error> {
        ctx.push_step(step).await;
        Ok(())
    }

    async fn run(
        &mut self,
        ctx: &StepContext<B, Compact>,
        args: Task<T, B::Meta, B::IdType>,
    ) -> Result<Self::Response, Self::Error> {
        let res = self.inner.call(args).await.map_err(|e| e.into())?;
        Ok(res)
    }
}

pub struct FilterMap<S, T, O> {
    mapper: PhantomData<FilterMapStep<S, T, O>>,
}

impl<S, T, O> Clone for FilterMap<S, T, O> {
    fn clone(&self) -> Self {
        FilterMap {
            mapper: self.mapper.clone(),
        }
    }
}

impl<S, T, O, E, B, Compact> Step<Vec<T>, B, Compact> for FilterMap<S, T, O>
where
    S: Service<Task<T, B::Meta, B::IdType>, Response = Option<O>, Error = E> + Sync + Send,
    Compact: DeserializeOwned + Sync + Serialize + Clone + Send,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    E: Into<BoxDynError>,
    O: Sync + Send,
    B: Sync + TaskSink<Compact> + Unpin + Send + WaitForCompletion<O, Compact>,
    T: Send,
    B::Meta: Send
        + Sync
        + MetadataExt<WorkflowRequest>
        + MetadataExt<FilterContext<B::IdType>>
        + Default,
    <B::Meta as MetadataExt<WorkflowRequest>>::Error: Debug,
    <B::Meta as MetadataExt<FilterContext<B::IdType>>>::Error: Debug,
    B::Error: Debug,
    B::IdType: Default + Clone + Send,
    T: Serialize,
{
    type Response = Vec<O>;
    type Error = BoxDynError;
    async fn pre(ctx: &mut StepContext<B, Compact>, steps: &Vec<T>) -> Result<(), Self::Error> {
        let mut task_ids = Vec::new();
        for step in steps {
            let task_id = ctx.push_step(step).await.unwrap();
            task_ids.push(task_id);
        }
        let mut meta = B::Meta::default();
        meta.inject(FilterContext { task_ids }).unwrap();
        meta.inject(WorkflowRequest {
            step_index: ctx.current_step + 1,
        })
        .unwrap();
        let task =
            TaskBuilder::new_with_metadata(serde_json::to_value(steps).unwrap(), meta).build();

        ctx.push_raw(task).await.unwrap();
        Ok(())
    }

    async fn run(
        &mut self,
        ctx: &StepContext<B, Compact>,
        steps: Task<Vec<T>, B::Meta, B::IdType>,
    ) -> Result<Self::Response, Self::Error> {
        let filter_ctx: FilterContext<B::IdType> = steps.ctx.metadata.extract().unwrap();
        let res = ctx
            .wait_for(&filter_ctx.task_ids)
            .await?
            .into_iter()
            .map(|res| res.result.unwrap())
            .collect();
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

impl<S, T, O, E, B, Compact> Step<T, B, Compact> for FilterMapStep<S, T, O>
where
    S: Service<Task<T, B::Meta, B::IdType>, Response = Option<O>, Error = E> + Sync + Send,
    T: DeserializeOwned + Sync,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    E: Into<BoxDynError>,
    O: Sync + Send,
    B: Sync + Send + TaskSink<Compact>,
    T: Send,
    B::Meta: Send + Sync,
    B::IdType: Send,
    Compact: Send + Sync
{
    type Response = S::Response;
    type Error = BoxDynError;

    async fn run(
        &mut self,
        ctx: &StepContext<B, Compact>,
        args: Task<T, B::Meta, B::IdType>,
    ) -> Result<Self::Response, Self::Error> {
        let res = self.inner.call(args).await.map_err(|e| e.into())?;
        Ok(res)
    }
    async fn post(
        &self,
        ctx: &mut StepContext<B, Compact>,
        res: &Self::Response,
    ) -> Result<bool, Self::Error> {
        Ok(false) // The parent task will handle the collection
    }
}

pub struct FilterStep<S, T> {
    inner: S,
    _marker: std::marker::PhantomData<T>,
}

impl<S, T> FilterStep<S, T> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            _marker: std::marker::PhantomData,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FilterContext<IdType = RandomId> {
    task_ids: Vec<TaskId<IdType>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WorkflowRequest {
    pub step_index: usize,
}

impl Default for WorkflowRequest {
    fn default() -> Self {
        WorkflowRequest { step_index: 0 }
    }
}

#[derive(Debug, Clone)]
pub struct StepContext<B, Compact> {
    current_step: usize,
    backend: B,
    _marker: PhantomData<Compact>
}
impl<B, Compact> StepContext<B, Compact> {
    fn new(backend: B, current_step: usize) -> Self {
        Self {
            current_step,
            backend,
            _marker: PhantomData
        }
    }

    async fn wait_for<O>(
        &self,
        task_ids: &Vec<TaskId<B::IdType>>,
    ) -> Result<Vec<TaskResult<O>>, B::Error>
    where
        O: Sync + Send,
        B: Sync + WaitForCompletion<O, Compact>,
        B::IdType: Clone,
    {
        let items = self
            .backend
            .wait_for(task_ids.clone())
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        Ok(items)
    }

    async fn push_raw<Args>(&mut self, task: Task<Args, B::Meta, B::IdType>) -> Result<(), B::Error>
    where
        B: Sync + TaskSink<Args> + Unpin,
        B::Meta: Send + Default + MetadataExt<WorkflowRequest>,
        B::Error: Debug,
        // Meta::Error: Debug,
    {
        self.backend.push_raw(task).await
    }

    async fn push_step_with_index<T>(
        &mut self,
        index: usize,
        step: &T,
    ) -> Result<TaskId<B::IdType>, B::Error>
    where
        T: DeserializeOwned + Sync + Serialize + Clone,
        B: Sync + TaskSink<T> + Unpin,
        T: Send,
        B::Meta: Send + Default + MetadataExt<WorkflowRequest>,
        B::Error: Debug,
        // Meta::Error: Debug,
        B::IdType: Default + Clone,
    {
        let task_id = TaskId::new(B::IdType::default());
        let mut meta = Meta::default();
        meta.inject(WorkflowRequest { step_index: index }).unwrap();
        let task = TaskBuilder::new_with_metadata(step.clone(), meta)
            .with_task_id(task_id.clone())
            .build();
        TaskSink::push_raw(&mut self.backend, task).await.unwrap();
        Ok(task_id)
    }

    async fn push_step<T>(&mut self, step: &T) -> Result<TaskId<B::IdType>, B::Error>
    where
        T: DeserializeOwned + Sync + Serialize + Clone,
        B: Sync + TaskSink<T> + Unpin,
        T: Send,
        B::Meta: Send + Default + MetadataExt<WorkflowRequest>,
        B::Error: Debug,
        // Meta::Error: Debug,
        B::IdType: Default + Clone,
    {
        self.push_step_with_index(self.current_step + 1, step).await
    }
}

// impl<S, T, Meta: MetadataExt<FilterContext>> Service<Task<serde_json::Value, Meta>>
//     for FilterStep<S, T>
// where
//     S: Service<Task<T, Meta>, Response = bool>,
//     T: DeserializeOwned,
//     S::Future: Send + 'static,
//     S::Error: Into<BoxDynError>,
// {
//     type Response = GoTo<serde_json::Value>;
//     type Error = BoxDynError;
//     type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

//     fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//         self.inner.poll_ready(cx).map_err(|e| e.into())
//     }

//     fn call(&mut self, req: Task<serde_json::Value, Meta>) -> Self::Future {
//         // let filter_ctx: FilterContext = req.ctx.metadata.extract().unwrap();
//         let mapped = req.map(|v| serde_json::from_value::<T>(v).unwrap());

//         let fut = self.inner.call(mapped);

//         Box::pin(async move {
//             let resp = fut.await.map_err(|e| e.into())?;
//             match resp {
//                 true => Ok(GoTo::Filter(resp)),
//                 false => Ok(GoTo::Done(serde_json::to_value(resp).unwrap())),
//             }
//         })
//     }
// }

impl<Args, Meta, Input, Current, Sink, Compact>
    WorkerServiceBuilder<Sink, WorkFlowService<Compact, Meta, Sink>, Args, Meta>
    for WorkFlow<Input, Current, Sink, Compact>
where
    Sink: Clone,
    Compact: Send,
    Sink: TaskSink<Compact>,
{
    fn build(self, b: &Sink) -> WorkFlowService<Compact, Meta, Sink> {
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

    use crate::{GoTo, WorkFlow};

    const ITEMS: u32 = 10;

    #[tokio::test]
    async fn it_works() {
        // let root =
        //     tower::service_fn(|req: Vec<u32>| async move { Ok::<_, Infallible>(req) }).boxed();
        // let not_found = tower::service_fn(|req| async move { Ok::<_, Infallible>(req) }).boxed();
        // let steer = Steer::new(vec![root, not_found], |task: &Vec<u32>, _services: &[_]| {
        //     if task.len() == 1 {
        //         0usize // Index of `root`
        //     } else {
        //         1 // Index of `not_found`
        //     }
        // });
        // Assuming we start at 0
        let workflow = WorkFlow::new("count_to_100")
            .then(|a: usize, wrk: WorkerContext| async move {
                if (a == 2) {
                    panic!("Boom!");
                }
                Ok::<_, BoxDynError>(vec![a, a + 1])
            }) // result: 1u32
            // .repeat(5, |a| async move { Ok::<_, BoxDynError>(a + 1) }) // result: Vec<2u32; 5>
            // .then(|a| async move { Ok::<_, BoxDynError>(a) })
            // .f // result: Vec<2u32; 5>
            // .filter(|a| async move { true }) // result: Vec<2u32; 5>
            .filter_map(|a| async move { Some(a * 2) }) // result: Vec<4u32; 5>
            .then(|res, wrk: WorkerContext| async move {
                dbg!(res);
                wrk.stop().unwrap();
            });

        dbg!(workflow.steps.len());

        // .then(|items: Vec<usize>| async move { Ok::<usize, BoxDynError>(items.len()) })
        // .repeat_until(|a| async move {
        //     // result: Vec<20u32; 5>
        //     if a < 20 {
        //         Ok(Some(20))
        //     } else {
        //         Ok::<_, BoxDynError>(None)
        //     }
        // });

        // .all(|a| async move { a > 3 }) // result: Vec<4u32; 5>
        // // .any(|a| async move { a == 3 }) // result: 3
        // .then(|items| async move { Ok::<u32, BoxDynError>(items.iter().sum()) })

        // .steer(steer);
        // result: 100u32

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
