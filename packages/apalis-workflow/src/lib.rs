use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures::{
    future::{ready, BoxFuture},
    FutureExt,
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
    task::{metadata::MetadataExt, task_id::TaskId, ExecutionContext, Task},
};

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
    type Error;
    fn pre(
        &self,
        ctx: &StepContext<Backend>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn run(
        &mut self,
        ctx: &StepContext<Backend>,
        args: Task<Args, Meta>,
    ) -> impl Future<Output = Result<Self::Response, Self::Error>> + Send;

    fn post(
        &self,
        ctx: &StepContext<Backend>,
        res: &Self::Response,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

// pub type BoxStep<Args> = Box<dyn Step<Args, Response = Value, Error = BoxDynError>>;

pub struct StepService<S, C, Args, B> {
    step: S,
    codec: PhantomData<(C, Args)>,
    backend: B,
}

impl<Args, Meta, S, C, Compact, B> Service<Task<Compact, Meta>> for StepService<S, C, Args, B>
where
    S: Step<Args, Meta, B> + Clone + Send + 'static,
    C: Codec<Args, Compact = Compact>,
    S::Response: Send + 'static,
    C::Error: Debug,
    S::Error: Debug + Send + 'static,
    B: Clone + Send + 'static,
    Args: Send + 'static,
    Meta: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<S::Response, S::Error>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: Task<Compact, Meta>) -> Self::Future {
        let req = req.try_map(|arg| C::decode(arg)).unwrap();

        let ctx = StepContext::new(self.backend.clone());

        let mut step = self.step.clone();
        Box::pin(async move {
            let pre = step.pre(&ctx).await.unwrap();
            let res = step.run(&ctx, req).await.unwrap();
            let post = step.post(&ctx, &res).await.unwrap();
            Ok(res)
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
type SteppedService<Compact, Meta> = BoxedService<Task<Compact, Meta>, GoTo<Compact>>;

pub struct WorkFlow<
    Input,
    Current,
    Compact = serde_json::Value,
    Meta = (),
    Backend = MemoryStorage<JsonMemory<serde_json::Value>>,
> {
    steps: HashMap<usize, Box<dyn FnOnce(Backend) -> SteppedService<Compact, Meta>>>,
    _marker: PhantomData<(Input, Current, Backend)>,
}

impl<Input> WorkFlow<Input, Input> {
    pub fn new(name: &str) -> Self {
        Self {
            steps: HashMap::new(),
            _marker: PhantomData,
        }
    }
}

impl<Input, Current, Meta> WorkFlow<Input, Current, serde_json::Value, Meta>
where
    Current: DeserializeOwned + Send + 'static,
{
    pub fn then<F, O, E, FnArgs>(mut self, then: F) -> WorkFlow<Input, O, serde_json::Value, Meta>
    where
        O: Serialize + Send + 'static,
        E: Into<BoxDynError> + Send + Sync + 'static,
        F: Send + 'static + Sync + Clone,
        ServiceFn<F, Current, Meta, FnArgs>: Service<Task<Current, Meta>, Response = O, Error = E>,
        FnArgs: std::marker::Send + 'static + Sync,
        Current: std::marker::Send + 'static + Serialize + Sync,
        Meta: Send + 'static + Sync,
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

    pub fn add_step< O, S>(mut self, step: S) -> WorkFlow<Input, O, serde_json::Value, Meta>
    where
        O: Serialize + Send + 'static,
        Current: std::marker::Send + 'static + Serialize + Sync,
        Meta: Send + 'static + Sync,
        S: Step<
                Current,
                Meta,
                MemoryStorage<JsonMemory<Value>>,
                Response = GoTo<Value>,
                Error = BoxDynError,
            > + Clone
            + Sync
            + Send
            + 'static,
        S::Response: Send,
        S::Error: Debug + Send,
    {
        self.steps.insert(
            self.steps.len(),
            Box::new(|backend: MemoryStorage<JsonMemory<Value>>| {
                SteppedService::<Value, Meta>::new(StepService {
                    backend,
                    codec: PhantomData::<(JsonCodec<Value>, Current)>,
                    step,
                })
            }),
        );
        WorkFlow {
            steps: self.steps,
            _marker: PhantomData,
        }
    }
}

impl<Input, Current, Meta> WorkFlow<Input, Vec<Current>, serde_json::Value, Meta>
where
    Current: DeserializeOwned + Send + 'static,
    Meta: MetadataExt<FilterContext> + Send + 'static,
{
    pub fn filter<F, FnArgs>(
        mut self,
        predicate: F,
    ) -> WorkFlow<Input, Vec<Current>, serde_json::Value, Meta>
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
    ) -> WorkFlow<Input, Vec<T>, serde_json::Value, Meta>
    where
        F: Send + 'static,
        ServiceFn<F, Current, Meta, FnArgs>: Service<Task<Current, Meta>, Response = Option<T>>,
        FnArgs: std::marker::Send + 'static,
        Current: std::marker::Send + 'static,
        // Meta: Send + 'static,
        <ServiceFn<F, Current, Meta, FnArgs> as Service<Task<Current, Meta>>>::Future:
            Send + 'static,
        <ServiceFn<F, Current, Meta, FnArgs> as Service<Task<Current, Meta>>>::Error:
            Into<BoxDynError>,
        T: Send + Serialize + 'static,
    {
        // self.steps.insert(
        //     self.steps.len(),
        //     SteppedService::<Value, Meta>::new(FilterMapStep {
        //         inner: service_fn::<F, Current, Meta, FnArgs>(predicate),
        //         _marker: PhantomData,
        //     }),
        // );
        WorkFlow {
            steps: self.steps,
            _marker: PhantomData,
        }
    }

    pub fn all<Fut, F>(mut self, next: F) -> WorkFlow<Input, Vec<Current>, serde_json::Value, Meta>
    where
        F: FnMut(Current) -> Fut,
        Fut: Future<Output = bool>,
    {
        // self.steps.insert(self.steps.len(), Box::new(next));
        WorkFlow {
            steps: self.steps,
            _marker: PhantomData,
        }
    }

    pub fn any<Fut, F>(mut self, next: F) -> WorkFlow<Input, bool, serde_json::Value, Meta>
    where
        F: FnMut(Current) -> Fut,
        Fut: Future<Output = bool>,
    {
        // self.steps.insert(self.steps.len(), Box::new(next));

        WorkFlow {
            steps: self.steps,
            _marker: PhantomData,
        }
    }
}
impl<Input, Current, Meta> WorkFlow<Input, Current, serde_json::Value, Meta> {
    pub fn repeat<FnArgs, F, O, E>(
        mut self,
        repeat: usize,
        next: F,
    ) -> WorkFlow<Input, Vec<O>, serde_json::Value, Meta>
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
    ) -> WorkFlow<Input, Vec<O>, serde_json::Value, Meta>
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
    T: DeserializeOwned + Sync,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    E: Into<BoxDynError>,
    O: Serialize,
    B: Sync,
    T: Send,
    Meta: Send,
{
    type Response = GoTo<serde_json::Value>;
    type Error = BoxDynError;
    async fn pre(&self, ctx: &StepContext<B>) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn run(
        &mut self,
        ctx: &StepContext<B>,
        args: Task<T, Meta>,
    ) -> Result<Self::Response, Self::Error> {
        let res = self.inner.call(args).await.map_err(|e| e.into())?;
        Ok(GoTo::Next(serde_json::to_value(res).unwrap()))
    }

    async fn post(&self, ctx: &StepContext<B>, res: &Self::Response) -> Result<(), Self::Error> {
        Ok(())
    }

    // type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    // fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
    //     self.inner.poll_ready(cx).map_err(|e| e.into())
    // }

    // fn call(&mut self, req: Task<serde_json::Value, Meta>) -> Self::Future {
    //     let mapped = req.map(|v| serde_json::from_value::<T>(v).unwrap());

    //     let fut = self.inner.call(mapped);

    //     Box::pin(async move {
    //         let resp = fut.await.map_err(|e| e.into())?;
    //         Ok(GoTo::Next(serde_json::to_value(resp).unwrap()))
    //     })
    // }
}

pub struct FilterMapStep<S, T, O> {
    inner: S,
    _marker: std::marker::PhantomData<(T, O)>,
}

impl<S, T, O> FilterMapStep<S, T, O> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S, T, O, Meta> Service<Task<serde_json::Value, Meta>> for FilterMapStep<S, T, O>
where
    S: Service<Task<T, Meta>, Response = Option<O>>,
    T: DeserializeOwned,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    O: Serialize,
{
    type Response = GoTo<serde_json::Value>;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: Task<serde_json::Value, Meta>) -> Self::Future {
        let mapped = req.map(|v| serde_json::from_value::<T>(v).unwrap());

        let fut = self.inner.call(mapped);

        Box::pin(async move {
            let resp = fut.await.map_err(|e| e.into())?;
            match resp {
                Some(resp) => Ok(GoTo::FilterMap(serde_json::to_value(resp).unwrap())),
                None => Ok(GoTo::Done(Value::Null)),
            }
        })
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

pub struct FilterContext {
    parent_id: TaskId,
    task_ids: Vec<TaskId>,
}

#[derive(Debug)]
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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, convert::Infallible};

    use apalis_core::{
        error::BoxDynError,
        service_fn::service_fn,
        task::{status::Status, Task},
        worker::context::WorkerContext,
    };
    use serde_json::Value;
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
            .then(|a: usize| async move { Ok::<_, BoxDynError>(a + 1) }) // result: 1u32
            .repeat(5, |a| async move { Ok::<_, BoxDynError>(a + 1) }) // result: Vec<2u32; 5>
            .then(|a| async move { Ok::<_, BoxDynError>(a) }) // result: Vec<2u32; 5>
            // .filter(|a| async move { true }) // result: Vec<2u32; 5>
            .filter_map(|a| async move { Some(a * 2) }) // result: Vec<4u32; 5>
            .then(|items: Vec<usize>| async move { Ok::<usize, BoxDynError>(items.len()) })
            .repeat_until(|a| async move {
                // result: Vec<20u32; 5>
                if a < 20 {
                    Ok(Some(20))
                } else {
                    Ok::<_, BoxDynError>(None)
                }
            });

        // .all(|a| async move { a > 3 }) // result: Vec<4u32; 5>
        // // .any(|a| async move { a == 3 }) // result: 3
        // .then(|items| async move { Ok::<u32, BoxDynError>(items.iter().sum()) })

        // .steer(steer);
        // result: 100u32
    }
}
