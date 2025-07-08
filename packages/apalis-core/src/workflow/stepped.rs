use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures::{future::BoxFuture, lock::Mutex, FutureExt, Sink, TryFutureExt};
use serde::{Deserialize, Serialize};
use tower::{
    layer::util::{Identity, Stack},
    util::{BoxService, MapRequest},
    Layer, Service, ServiceBuilder, ServiceExt,
};

use crate::{
    backend::{
        codec::{Decoder, Encoder},
        Backend, TaskSink,
    },
    error::BoxDynError,
    request::{Parts, Request},
    service_fn::{service_fn, ServiceFn},
    worker::{
        builder::{WorkerBuilder, WorkerFactory},
        Worker,
    },
    workflow::GoTo,
};

type BoxedService<Input, Output> = tower::util::BoxService<Input, Output, BoxDynError>;

type SteppedService<Compact, Ctx> = BoxedService<Request<Compact, Ctx>, GoTo<Compact>>;

type RouteService<Compact, Ctx> = BoxedService<Request<Compact, Ctx>, ()>;

// The request type that carries step information
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct StepRequest<T> {
    pub index: usize,
    pub step: T,
}

impl<T> StepRequest<T> {
    pub fn new(index: usize, inner: T) -> Self {
        Self { index, step: inner }
    }
}

#[derive(Clone)]
pub struct StepBuilder<Input, Current, Svc, Codec> {
    steps: HashMap<usize, Svc>,
    input: PhantomData<Input>,
    current: PhantomData<Current>,
    codec: PhantomData<Codec>,
}

impl<Input, Compact, Ctx, Codec> StepBuilder<Input, Input, SteppedService<Compact, Ctx>, Codec> {
    fn new() -> StepBuilder<Input, Input, SteppedService<Compact, Ctx>, Codec> {
        StepBuilder {
            steps: HashMap::new(),
            input: PhantomData,
            current: PhantomData,
            codec: PhantomData,
        }
    }
}

impl<Input, Current, Compact, Codec, Ctx>
    StepBuilder<Input, Current, SteppedService<Compact, Ctx>, Codec>
{
    pub fn step<S, Next, E: Into<BoxDynError>>(
        mut self,
        service: S,
    ) -> StepBuilder<Input, Next, SteppedService<Compact, Ctx>, Codec>
    where
        S: Service<Request<Current, Ctx>, Response = GoTo<Next>, Error = E> + Send + 'static,
        S::Future: Send + 'static,
        Codec: Encoder<Next, Compact = Compact>
            + Decoder<Current, Compact = Compact>
            + Encoder<Next, Compact = Compact>,
        <Codec as Decoder<Current>>::Error: Debug,
        <Codec as Encoder<Next>>::Error: Debug,
    {
        let current_index = self.steps.len();
        let svc = ServiceBuilder::new()
            .map_request(|req: Request<Compact, Ctx>| {
                Request::new_with_parts(
                    Codec::decode(&req.args).expect(&format!(
                        "Could not decode step, expecting {}",
                        std::any::type_name::<Current>()
                    )),
                    req.parts,
                )
            })
            .map_response(move |res| match res {
                GoTo::Next(next) => {
                    GoTo::Next(Codec::encode(&next).expect("Could not encode the next step"))
                }
                GoTo::Delay { next, delay } => GoTo::Delay {
                    next: Codec::encode(&next).expect("Could not encode the next step"),
                    delay,
                },
                GoTo::Done(res) => {
                    GoTo::Done(Codec::encode(&res).expect("Could not encode the next step"))
                }
            })
            .map_err(|e: E| e.into())
            .service(service);
        self.steps.insert(current_index, BoxedService::new(svc));
        StepBuilder {
            steps: self.steps,
            input: self.input,
            current: PhantomData,
            codec: PhantomData,
        }
    }
    pub fn step_fn<F, Next, E: Into<BoxDynError>, FnArgs>(
        self,
        step_fn: F,
    ) -> StepBuilder<Input, Next, SteppedService<Compact, Ctx>, Codec>
    where
        Codec: Encoder<Next, Compact = Compact> + Decoder<Current, Compact = Compact>,
        <Codec as Decoder<Current>>::Error: Debug,
        <Codec as Encoder<Next>>::Error: Debug,
        F: Send + 'static,
        ServiceFn<F, Current, Ctx, FnArgs>:
            Service<Request<Current, Ctx>, Response = GoTo<Next>, Error = E>,
        FnArgs: std::marker::Send + 'static,
        Current: std::marker::Send + 'static,
        Ctx: Send + 'static,
        <ServiceFn<F, Current, Ctx, FnArgs> as Service<Request<Current, Ctx>>>::Future:
            Send + 'static,
    {
        self.step(service_fn::<F, Current, Ctx, FnArgs>(step_fn))
    }
}

#[derive(Clone)]
pub struct StepService<S> {
    step_service: S,
    step_index: usize,
}

impl<Compact, Ctx, B, M, Input, Current, Cdc>
    WorkerFactory<Compact, Ctx, RouteService<Compact, Ctx>, B, M>
    for StepBuilder<Input, Current, SteppedService<Compact, Ctx>, Cdc>
where
    B: Backend<Request<Compact, Ctx>>,
    M: Layer<RouteService<Compact, Ctx>>,
    Input: Send + 'static,
    Current: Send + 'static,
    Cdc: Send
        + 'static
        + Decoder<StepRequest<Compact>, Compact = Compact>
        + Encoder<StepRequest<Compact>, Compact = Compact>,
    <Cdc as Encoder<StepRequest<Compact>>>::Error: Debug,
    <Cdc as Decoder<StepRequest<Compact>>>::Error: Debug,
    Ctx: 'static,
    Compact: 'static + Send,
    B::Sink: TaskSink<Compact, Codec = Cdc, Compact = Compact> + Sync + 'static,
{
    fn service(self, backend: &B) -> RouteService<Compact, Ctx> {
        let sink = backend.sink();
        BoxedService::new(RoutedStepService {
            inner: self,
            sink: Arc::new(Mutex::new(sink)),
        })
    }
}

pub struct RoutedStepService<Inner, SinkT> {
    inner: Inner,
    sink: Arc<Mutex<SinkT>>,
}

impl<Compact, Ctx, Input, Current, Cdc, SinkT> Service<Request<Compact, Ctx>>
    for RoutedStepService<StepBuilder<Input, Current, SteppedService<Compact, Ctx>, Cdc>, SinkT>
where
    Compact: Send + 'static,
    Cdc: Decoder<StepRequest<Compact>, Compact = Compact>
        + Encoder<StepRequest<Compact>, Compact = Compact>,
    <Cdc as Encoder<StepRequest<Compact>>>::Error: Debug,
    <Cdc as Decoder<StepRequest<Compact>>>::Error: Debug,
    SinkT: TaskSink<Compact, Compact = Compact, Codec = Cdc> + 'static,
{
    type Response = ();
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<(), BoxDynError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Compact, Ctx>) -> Self::Future {
        let req: Request<StepRequest<Compact>, _> = request.map(|c| Cdc::decode(&c).unwrap());
        let index = req.args.index;
        let svc = self.inner.steps.get_mut(&index).unwrap();
        let sink = self.sink.clone();
        let fut = svc
            .call(req.map(|r| r.step))
            .and_then(move |res| async move {
                let mut sink = sink.lock().await;
                match res {
                    GoTo::Next(s) => {
                        let req = Request::new(StepRequest::new(index + 1, s))
                            .map(|req| Cdc::encode(&req).unwrap());
                        let _res = sink.push_raw_request(req).await;
                        Ok(())
                    }
                    GoTo::Delay { next, delay } => todo!(),
                    GoTo::Done(_) => Ok(()),
                }
            });

        fut.boxed()
    }
}

// Helper trait for easier step pushing
pub trait StepSinkExt<Step, Ctx, Starter> {
    type Error;

    /// Push a step request by ID and value
    fn push_step(
        &mut self,
        id: usize,
        value: Step,
    ) -> impl Future<Output = Result<Parts<Ctx>, Self::Error>>;

    /// Push the start step (ID 0) with a value
    fn push_start(&mut self, value: Step) -> impl Future<Output = Result<Parts<Ctx>, Self::Error>>;
}

impl<S, Step, Starter, Compact, Ctx, Err> StepSinkExt<Step, Ctx, Starter> for S
where
    S: TaskSink<StepRequest<Starter>, Compact = Compact, Context = Ctx, Error = Err>
        + Sink<Request<Compact, Ctx>>,
    Err: std::error::Error,
    S::Codec: Encoder<StepRequest<Starter>, Compact = Compact>,
    Ctx: Default,
{
    type Error = Err;

    async fn push_step(&mut self, id: usize, input: Starter) -> Result<Parts<Ctx>, Self::Error> {
        self.push(StepRequest::new(id, input)).await
    }

    async fn push_start(&mut self, input: Starter) -> Result<Parts<Ctx>, Self::Error> {
        self.push_step(0, input).await
    }
}

#[cfg(test)]
mod tests {

    use std::io;

    use futures::StreamExt;
    use tower::util::BoxService;

    use crate::{
        backend::{codec::json::JsonCodec, memory::MemoryStorage},
        request::data::Data,
        service_fn::service_fn,
        worker::{context::WorkerContext, ext::event_listener::EventListenerExt},
    };

    use super::*;

    #[tokio::test]
    async fn it_works() {
        async fn task1(job: u32) -> Result<GoTo<()>, io::Error> {
            println!("{job}");
            Ok(GoTo::Next(()))
        }

        async fn task2(_: ()) -> Result<GoTo<usize>, io::Error> {
            Ok(GoTo::Next(1))
        }

        async fn task3(job: usize, wrk: WorkerContext) -> Result<GoTo<()>, io::Error> {
            wrk.stop().unwrap();
            println!("{job}");
            Ok(GoTo::Done(()))
        }

        let stepper = StepBuilder::new()
            .step_fn(task1)
            .step_fn(task2)
            .step_fn(task3);

        let in_memory = MemoryStorage::new_with_json();
        let mut sink = in_memory.sink();
        let _res = sink.push_start(2u32).await.unwrap();

        let _res = sink.push_step(2, 9usize).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .on_event(|ctx, ev| {
                println!("Worker {:?}, On Event = {:?}", ctx.name(), ev);
            })
            .build(stepper);
        let mut event_stream = worker.stream();
        while let Some(Ok(ev)) = event_stream.next().await {
            println!("On Event = {:?}", ev);
        }
    }
}
