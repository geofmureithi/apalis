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
    FutureExt, Sink, Stream,
};
// use futures::{channel::mpsc::Receiver, stream::BoxStream, StreamExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use tower::{steer::Picker, util::BoxService, Service, ServiceBuilder, ServiceExt};

use apalis_core::{
    backend::{
        codec::{json::JsonCodec, Codec},
        memory::{JsonMemory, MemoryStorage},
        TaskSink,
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
pub trait Step<Args, Meta, Backend> {
    type Response;
    type Error: Send;
    fn pre(
        ctx: &mut StepContext<Backend>,
        step: &Args,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        ready(Ok(()))
    }

    fn run(
        &mut self,
        ctx: &StepContext<Backend>,
        step: Task<Args, Meta>,
    ) -> impl Future<Output = Result<Self::Response, Self::Error>> + Send;

    fn post(
        &self,
        ctx: &mut StepContext<Backend>,
        res: &Self::Response,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        ready(Ok(()))
    }
}

// pub type BoxStep<Args> = Box<dyn Step<Args, Response = Value, Error = BoxDynError>>;

pub struct StepService<S, C, Args, Meta, B>
where
    S: Step<Args, Meta, B>,
{
    step: S,
    codec: PhantomData<(C, Args, Meta)>,
    backend: B,
}

impl<Args, Meta, S, C, Compact, B> Service<Task<Compact, Meta>> for StepService<S, C, Args, Meta, B>
where
    S: Step<Args, Meta, B> + Clone + Send + 'static,
    C: Codec<Args, Compact = Compact> + Codec<S::Response, Compact = Compact>,
    S::Response: Send + 'static,
    <C as Codec<Args>>::Error: Debug,
    <C as Codec<S::Response>>::Error: Debug,
    S::Error: Debug + Send + 'static,
    B: Clone + Send + 'static,
    Args: Send + 'static,
    Meta: Send + 'static,
{
    type Response = Compact;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Compact, S::Error>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: Task<Compact, Meta>) -> Self::Future {
        let req = req.try_map(|arg| C::decode(arg)).unwrap();

        let mut ctx = StepContext::new(self.backend.clone());

        let mut step = self.step.clone();
        Box::pin(async move {
            let res = step.run(&ctx, req).await.unwrap();
            let _ = step.post(&mut ctx, &res).await.unwrap();
            Ok(C::encode(&res).unwrap())
        })
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
type SteppedService<Compact, Meta> = BoxedService<Task<Compact, Meta>, Compact>;

pub struct CompositeService<Compact, Meta, Backend> {
    pre_hook: Arc<
        Box<
            dyn Fn(&StepContext<Backend>, &Compact) -> BoxFuture<'static, Result<(), BoxDynError>>
                + Send
                + Sync
                + 'static,
        >,
    >,
    svc: SteppedService<Compact, Meta>,
}

pub struct WorkFlow<Input, Current, Meta, Backend, Compact = serde_json::Value> {
    steps: HashMap<usize, Box<dyn FnOnce(Backend) -> CompositeService<Compact, Meta, Backend>>>,
    _marker: PhantomData<(Input, Current, Backend)>,
}

impl<Input, Meta, Backend> WorkFlow<Input, Input, Meta, Backend> {
    pub fn new(name: &str) -> Self {
        Self {
            steps: HashMap::new(),
            _marker: PhantomData,
        }
    }
}

impl<Input, Current, Meta, Backend> WorkFlow<Input, Current, Meta, Backend, serde_json::Value>
where
    Current: DeserializeOwned + Send + 'static,
    Backend: Send + Clone + Sync + 'static + Unpin + futures::Sink<Task<Value, Meta>>,
{
    pub fn then<F, O, E, FnArgs>(
        mut self,
        then: F,
    ) -> WorkFlow<Input, O, Meta, Backend, serde_json::Value>
    where
        O: Serialize + DeserializeOwned + Sync + Send + 'static,
        E: Into<BoxDynError> + Send + Sync + 'static,
        F: Send + 'static + Sync + Clone,
        ServiceFn<F, Current, Meta, FnArgs>: Service<Task<Current, Meta>, Response = O, Error = E>,
        FnArgs: std::marker::Send + 'static + Sync,
        Current: std::marker::Send + 'static + Serialize + Sync + Debug,
        Meta: Send + Sync + Default + 'static,
        Backend::Error: Debug,
        <ServiceFn<F, Current, Meta, FnArgs> as Service<Task<Current, Meta>>>::Future:
            Send + 'static,
        <ServiceFn<F, Current, Meta, FnArgs> as Service<Task<Current, Meta>>>::Error:
            Into<BoxDynError>,
    {
        self.add_step(ThenStep {
            inner: service_fn::<F, Current, Meta, FnArgs>(then),
            _marker: PhantomData,
        })
    }

    pub fn add_step<O, S, Res>(
        mut self,
        step: S,
    ) -> WorkFlow<Input, O, Meta, Backend, serde_json::Value>
    where
        O: Send + 'static,
        Current: std::marker::Send + 'static + Serialize + Sync,
        Meta: Send + 'static + Sync,
        S: Step<Current, Meta, Backend, Response = Res, Error = BoxDynError>
            + Clone
            + Sync
            + Send
            + 'static,
        S::Response: Send,
        S::Error: Debug + Send,
        Res: Serialize + DeserializeOwned + 'static,
    {
        self.steps.insert(
            self.steps.len() + 1,
            Box::new(|backend: Backend| {
                let pre_hook = Arc::new(Box::new(move |ctx: &StepContext<Backend>, step: &Value| {
                    let val = JsonCodec::<Value>::decode(step.clone()).unwrap();
                    let mut ctx = ctx.clone();
                    async move {
                        S::pre(&mut ctx, &val).await?;
                        Ok(())
                    }
                    .boxed()
                })
                    as Box<
                        dyn Fn(
                                &StepContext<Backend>,
                                &Value,
                            )
                                -> BoxFuture<'static, Result<(), BoxDynError>>
                            + Send
                            + Sync
                            + 'static,
                    >);
                let svc = SteppedService::<Value, Meta>::new(StepService {
                    backend,
                    codec: PhantomData::<(JsonCodec<Value>, Current, Meta)>,
                    step,
                });
                CompositeService { pre_hook, svc }
            }),
        );
        WorkFlow {
            steps: self.steps,
            _marker: PhantomData,
        }
    }
}

impl<Input, Current, Meta, Backend> WorkFlow<Input, Vec<Current>, Meta, Backend, serde_json::Value>
where
    Current: DeserializeOwned + Send + 'static,
    Meta: MetadataExt<FilterContext> + Send + 'static,
{
    pub fn filter<F, FnArgs>(
        mut self,
        predicate: F,
    ) -> WorkFlow<Input, Vec<Current>, Meta, Backend, serde_json::Value>
    where
        F: Send + 'static,
        ServiceFn<F, Current, Meta, FnArgs>: Service<Task<Current, Meta>, Response = bool>,
        FnArgs: std::marker::Send + 'static,
        Current: std::marker::Send + 'static,
        <ServiceFn<F, Current, Meta, FnArgs> as Service<Task<Current, Meta>>>::Future:
            Send + 'static,
        <ServiceFn<F, Current, Meta, FnArgs> as Service<Task<Current, Meta>>>::Error:
            Into<BoxDynError>,
    {
        let current_step = self.steps.len() + 1;
        // self.steps.insert(
        //     current_step,
        //     SteppedService::<Value, Meta>::new(FilterStep {
        //         inner: service_fn::<F, Current, Meta, FnArgs>(predicate),
        //         _marker: PhantomData,
        //     }),
        // );

        WorkFlow {
            steps: self.steps,
            _marker: PhantomData,
        }
    }

    pub fn filter_map<F, T, FnArgs>(
        mut self,
        predicate: F,
    ) -> WorkFlow<Input, Vec<T>, Meta, Backend, serde_json::Value>
    where
        F: Send + 'static + Sync,
        ServiceFn<F, Current, Meta, FnArgs>:
            Service<Task<Current, Meta>, Response = Option<T>> + Clone,
        FnArgs: std::marker::Send + 'static,
        Current: std::marker::Send + 'static + Serialize + Sync + Debug,
        Meta: Send + 'static + Sync + Default,
        <ServiceFn<F, Current, Meta, FnArgs> as Service<Task<Current, Meta>>>::Future:
            Send + 'static,
        <ServiceFn<F, Current, Meta, FnArgs> as Service<Task<Current, Meta>>>::Error:
            Into<BoxDynError>,
        T: Send + Serialize + 'static + Sync + DeserializeOwned,
        Meta::Error: Debug,
        FnArgs: Sync,
        Backend: Sync + Clone + Send + 'static + Unpin + futures::Sink<Task<Value, Meta>>,
        Backend::Error: Debug,
    {
        let parent = FilterMap {
            mapper: PhantomData::<FilterMapStep<ServiceFn<F, Current, Meta, FnArgs>, Current, T>>,
        };
        let mapper = FilterMapStep {
            inner: service_fn::<F, Current, Meta, FnArgs>(predicate),
            _marker: PhantomData,
        };
        self.add_step(parent).add_step(mapper)
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
impl<Input, Current, Meta, Backend> WorkFlow<Input, Current, Meta, Backend, serde_json::Value> {
    pub fn repeat<FnArgs, F, O, E>(
        mut self,
        repeat: usize,
        next: F,
    ) -> WorkFlow<Input, Vec<O>, Meta, Backend, serde_json::Value>
    where
        F: Send + 'static,
        ServiceFn<F, Current, Meta, FnArgs>: Service<Task<Current, Meta>, Response = O, Error = E>,
        FnArgs: std::marker::Send + 'static,
        Current: std::marker::Send + 'static,
        // Meta: Send + 'static,
        <ServiceFn<F, Current, Meta, FnArgs> as Service<Task<Current, Meta>>>::Future:
            Send + 'static,
        <ServiceFn<F, Current, Meta, FnArgs> as Service<Task<Current, Meta>>>::Error:
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
    ) -> WorkFlow<Input, Vec<O>, Meta, Backend, serde_json::Value>
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

impl<S, T, O, E, Meta, B> Step<T, Meta, B> for ThenStep<S, T>
where
    S: Service<Task<T, Meta>, Response = O, Error = E> + Sync + Send,
    T: DeserializeOwned + Sync + Serialize,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    E: Into<BoxDynError>,
    O: Sync,
    B: Sync + Unpin + Sink<Task<Value, Meta>> + Send,
    T: Send + Debug,
    Meta: Send + Sync + Default,
    B::Error: Debug,
{
    type Response = S::Response;
    type Error = BoxDynError;
    async fn pre(ctx: &mut StepContext<B>, step: &T) -> Result<(), Self::Error> {
        ctx.send(step).await;
        Ok(())
    }

    async fn run(
        &mut self,
        ctx: &StepContext<B>,
        args: Task<T, Meta>,
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

impl<S, T, O, E, Meta, B> Step<Vec<T>, Meta, B> for FilterMap<S, T, O>
where
    S: Service<Task<T, Meta>, Response = Option<O>, Error = E> + Sync + Send,
    T: DeserializeOwned + Sync + Serialize,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    E: Into<BoxDynError>,
    O: Sync + Send,
    B: Sync + futures::Sink<Task<Value, Meta>> + Unpin + Send,
    T: Send + Debug,
    Meta: Send + Sync + MetadataExt<FilterContext> + Default,
    Meta::Error: Debug,
    B::Error: Debug,
{
    type Response = Vec<O>;
    type Error = BoxDynError;
    async fn pre(ctx: &mut StepContext<B>, steps: &Vec<T>) -> Result<(), Self::Error> {
        let mut task_ids = Vec::new();
        for step in steps {
            let task_id = ctx.send(step).await;
            task_ids.push(task_id);
        }
        // let task = TaskBuilder::new(task_ids).build();
        // push
        Ok(())
    }

    async fn run(
        &mut self,
        ctx: &StepContext<B>,
        steps: Task<Vec<T>, Meta>,
    ) -> Result<Self::Response, Self::Error> {
        let filter_ctx = steps.ctx.metadata.extract().unwrap();
        let res = ctx.wait_for_results(&filter_ctx.task_ids).await;
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

impl<S, T, O, E, Meta, B> Step<T, Meta, B> for FilterMapStep<S, T, O>
where
    S: Service<Task<T, Meta>, Response = Option<O>, Error = E> + Sync + Send,
    T: DeserializeOwned + Sync,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    E: Into<BoxDynError>,
    O: Sync + Send,
    B: Sync,
    T: Send,
    Meta: Send + Sync,
{
    type Response = S::Response;
    type Error = BoxDynError;

    async fn run(
        &mut self,
        ctx: &StepContext<B>,
        args: Task<T, Meta>,
    ) -> Result<Self::Response, Self::Error> {
        let res = self.inner.call(args).await.map_err(|e| e.into())?;
        Ok(res)
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
pub struct FilterContext {
    task_ids: Vec<TaskId>,
}

#[derive(Debug, Clone)]
pub struct StepContext<B> {
    current_step: usize,
    backend: B,
}
impl<B> StepContext<B> {
    fn new(backend: B) -> Self {
        Self {
            current_step: 0,
            backend,
        }
    }

    async fn wait_for_results<O>(&self, task_ids: &Vec<TaskId>) -> Vec<O>
    where
        O: Sync + Send,
        B: Sync,
    {
        todo!()
    }

    async fn send<T, Meta>(&mut self, step: &T) -> TaskId
    where
        T: DeserializeOwned + Sync + Debug + Serialize,
        B: Sync + Sink<Task<Value, Meta>> + Unpin,
        T: Send,
        Meta: Send + Default,
        B::Error: Debug,
    {
        let task_id = TaskId::new(RandomId::default());
        let task = TaskBuilder::new(serde_json::to_value(step).unwrap())
            .with_task_id(task_id.clone())
            .build();
        futures::SinkExt::send(&mut self.backend, task)
            .await
            .unwrap();
        task_id
    }
}

impl<S, T, Meta: MetadataExt<FilterContext>> Service<Task<serde_json::Value, Meta>>
    for FilterStep<S, T>
where
    S: Service<Task<T, Meta>, Response = bool>,
    T: DeserializeOwned,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
{
    type Response = GoTo<serde_json::Value>;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: Task<serde_json::Value, Meta>) -> Self::Future {
        // let filter_ctx: FilterContext = req.ctx.metadata.extract().unwrap();
        let mapped = req.map(|v| serde_json::from_value::<T>(v).unwrap());

        let fut = self.inner.call(mapped);

        Box::pin(async move {
            let resp = fut.await.map_err(|e| e.into())?;
            match resp {
                true => Ok(GoTo::Filter(resp)),
                false => Ok(GoTo::Done(serde_json::to_value(resp).unwrap())),
            }
        })
    }
}

impl<Args, Meta, Input, Current, Backend, Compact>
    WorkerServiceBuilder<Backend, WorkFlowService<Compact, Meta, Backend>, Args, Meta>
    for WorkFlow<Input, Current, Meta, Backend, Compact>
where
    Backend: Clone,
{
    fn build(self, b: &Backend) -> WorkFlowService<Compact, Meta, Backend> {
        let services: HashMap<usize, _> = self
            .steps
            .into_iter()
            .map(|(index, svc)| (index, svc(b.clone())))
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
            .then(|a: usize| async move {
                Ok::<_, BoxDynError>(vec![a, a + 1])
            }) // result: 1u32
            // .repeat(5, |a| async move { Ok::<_, BoxDynError>(a + 1) }) // result: Vec<2u32; 5>
            // .then(|a| async move { Ok::<_, BoxDynError>(a) })
            // .f // result: Vec<2u32; 5>
            // .filter(|a| async move { true }) // result: Vec<2u32; 5>
            .filter_map(|a| async move { 
                if (a == 1) {
                    panic!("Boom!");
                }
                Some(a * 2) }); // result: Vec<4u32; 5>

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

        in_memory
            .push(Value::Number(Number::from_i128(1).unwrap()))
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
