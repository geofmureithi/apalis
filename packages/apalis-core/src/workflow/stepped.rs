use std::{
    collections::HashMap,
    fmt::Debug,
    future::{ready, Future},
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

pub struct StepBuilder<Input, Current, Svc, Codec, Compact, Ctx> {
    steps: HashMap<usize, Svc>,
    input: PhantomData<Input>,
    current: PhantomData<Current>,
    codec: PhantomData<Codec>,
    fallback: Option<
        Box<
            dyn FnMut(Request<Compact, Ctx>) -> BoxFuture<'static, Result<(), BoxDynError>>
                + Send
                + Sync
                + 'static,
        >,
    >,
}

impl<Input, Compact, Ctx, Codec>
    StepBuilder<Input, Input, SteppedService<Compact, Ctx>, Codec, Compact, Ctx>
{
    pub fn new() -> StepBuilder<Input, Input, SteppedService<Compact, Ctx>, Codec, Compact, Ctx> {
        StepBuilder {
            steps: HashMap::new(),
            input: PhantomData,
            current: PhantomData,
            codec: PhantomData,
            fallback: None,
        }
    }
}

impl<Input, Current, Compact, Ctx, Codec>
    StepBuilder<Input, Current, SteppedService<Compact, Ctx>, Codec, Compact, Ctx>
{
    pub fn fallback<F: Future<Output = Result<(), E>> + Send + 'static, E: Into<BoxDynError>>(
        mut self,
        mut fun: impl FnMut(Request<Compact, Ctx>) -> F + Send + Sync + 'static,
    ) -> Self {
        self.fallback = Some(Box::new(move |r| fun(r).map_err(|e| e.into()).boxed()));
        self
    }
}

pub struct SingleStepService<S, Codec, Current, Next, Compact, Ctx> {
    inner: S,
    _marker: PhantomData<(Codec, Current, Next, Compact, Ctx)>,
}

impl<S, Codec, Current, Next, Compact, Ctx>
    SingleStepService<S, Codec, Current, Next, Compact, Ctx>
{
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }
}

impl<S, Codec, Current, Next, Compact, Ctx, E> Service<Request<Compact, Ctx>>
    for SingleStepService<S, Codec, Current, Next, Compact, Ctx>
where
    S: Service<Request<Current, Ctx>, Response = GoTo<Next>, Error = E> + Send + 'static,
    S::Future: Send + 'static,
    Codec: Decoder<Current, Compact = Compact> + Encoder<Next, Compact = Compact>,
    Codec: Encoder<Next, Compact = Compact>,
    <Codec as Decoder<Current>>::Error: std::error::Error + Send + Sync + 'static,
    <Codec as Encoder<Next>>::Error: std::error::Error + Send + Sync + 'static,
    E: Into<BoxDynError>,
    Compact: Send + 'static,
{
    type Response = GoTo<Compact>;
    type Error = BoxDynError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<Compact, Ctx>) -> Self::Future {
        // Decode request
        let decoded = Codec::decode(&req.args);
        match decoded {
            Ok(decoded) => {
                let req = Request::new_with_parts(decoded, req.parts);
                let fut = self.inner.call(req);

                Box::pin(async move {
                    let res = fut.await.map_err(Into::into)?;

                    let encoded = match res {
                        GoTo::Next(next) => GoTo::Next(Codec::encode(&next).map_err(|e| {
                            BoxDynError::from(StepError::CodecError(
                                e.into(),
                                std::any::type_name::<Next>(),
                            ))
                        })?),
                        GoTo::Delay { next, delay } => GoTo::Delay {
                            next: Codec::encode(&next).map_err(|e| {
                                BoxDynError::from(StepError::CodecError(
                                    e.into(),
                                    std::any::type_name::<Next>(),
                                ))
                            })?,
                            delay,
                        },
                        GoTo::Done(done) => GoTo::Done(Codec::encode(&done).map_err(|e| {
                            BoxDynError::from(StepError::CodecError(
                                e.into(),
                                std::any::type_name::<Next>(),
                            ))
                        })?),
                    };

                    Ok(encoded)
                })
            }
            Err(e) => {
                let current = std::any::type_name::<Current>();
                ready(Err(BoxDynError::from(StepError::CodecError(
                    e.into(),
                    current,
                ))))
                .boxed()
            }
        }
    }
}

impl<Input, Current, Compact, Codec, Ctx>
    StepBuilder<Input, Current, SteppedService<Compact, Ctx>, Codec, Compact, Ctx>
where
    Ctx: Send + 'static,
    Current: Send + 'static,
    Compact: Send + 'static,
{
    pub fn step<S, Next, E: Into<BoxDynError>>(
        mut self,
        service: S,
    ) -> StepBuilder<Input, Next, SteppedService<Compact, Ctx>, Codec, Compact, Ctx>
    where
        S: Service<Request<Current, Ctx>, Response = GoTo<Next>, Error = E> + Send + 'static,
        S::Future: Send + 'static,
        Codec: Encoder<Next, Compact = Compact>
            + Decoder<Current, Compact = Compact>
            + Encoder<Next, Compact = Compact>,
        <Codec as Decoder<Current>>::Error: std::error::Error + Send + Sync + 'static,
        <Codec as Encoder<Next>>::Error: std::error::Error + Send + Sync + 'static,
        Next: Send + 'static,
        Codec: Send + 'static,
    {
        let current_index = self.steps.len();
        self.steps.insert(
            current_index,
            BoxedService::new(
                SingleStepService::<S, Codec, Current, Next, Compact, Ctx>::new(service),
            ),
        );
        StepBuilder {
            steps: self.steps,
            input: self.input,
            current: PhantomData,
            codec: PhantomData,
            fallback: self.fallback,
        }
    }
    pub fn step_fn<F, Next, E: Into<BoxDynError>, FnArgs>(
        self,
        step_fn: F,
    ) -> StepBuilder<Input, Next, SteppedService<Compact, Ctx>, Codec, Compact, Ctx>
    where
        Codec: Encoder<Next, Compact = Compact> + Decoder<Current, Compact = Compact>,
        <Codec as Decoder<Current>>::Error: std::error::Error + Send + Sync + 'static,
        <Codec as Encoder<Next>>::Error: std::error::Error + Send + Sync + 'static,
        F: Send + 'static,
        ServiceFn<F, Current, Ctx, FnArgs>:
            Service<Request<Current, Ctx>, Response = GoTo<Next>, Error = E>,
        FnArgs: std::marker::Send + 'static,
        Current: std::marker::Send + 'static,
        Ctx: Send + 'static,
        <ServiceFn<F, Current, Ctx, FnArgs> as Service<Request<Current, Ctx>>>::Future:
            Send + 'static,
        Next: Send + 'static,
        Codec: Send + 'static,
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
    WorkerFactory<StepRequest<Compact>, Ctx, RouteService<StepRequest<Compact>, Ctx>, B, M>
    for StepBuilder<Input, Current, SteppedService<Compact, Ctx>, Cdc, Compact, Ctx>
where
    B: Backend<StepRequest<Compact>, Ctx>,
    M: Layer<RouteService<Compact, Ctx>>,
    Input: Send + 'static,
    Current: Send + 'static,
    Cdc: Send
        + 'static
        + Decoder<StepRequest<Compact>, Compact = Compact>
        + Encoder<StepRequest<Compact>, Compact = Compact>,
    <Cdc as Encoder<StepRequest<Compact>>>::Error: std::error::Error + Send + Sync + 'static,
    <Cdc as Decoder<StepRequest<Compact>>>::Error: std::error::Error + Send + Sync + 'static,
    Ctx: 'static,
    Compact: 'static + Send,
    B::Sink: TaskSink<StepRequest<Compact>, Codec = Cdc, Compact = Compact, Context = Ctx>
        + Sync
        + 'static,
{
    fn service(self, backend: &B) -> RouteService<StepRequest<Compact>, Ctx> {
        let sink = backend.sink();
        BoxedService::new(RoutedStepService {
            inner: self,
            sink: Arc::new(Mutex::new(sink)),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StepError {
    #[error("The service at index `{0}` is not available")]
    OutOfBound(usize),
    #[error("Codec failed for step `{0}` expecting type {1}")]
    CodecError(BoxDynError, &'static str),
}

pub struct RoutedStepService<Inner, SinkT> {
    inner: Inner,
    sink: Arc<Mutex<SinkT>>,
}

impl<Compact, Ctx, Input, Current, Cdc, SinkT> Service<Request<StepRequest<Compact>, Ctx>>
    for RoutedStepService<
        StepBuilder<Input, Current, SteppedService<Compact, Ctx>, Cdc, Compact, Ctx>,
        SinkT,
    >
where
    Compact: Send + 'static,
    Cdc: Decoder<StepRequest<Compact>, Compact = Compact>
        + Encoder<StepRequest<Compact>, Compact = Compact>,
    <Cdc as Encoder<StepRequest<Compact>>>::Error: std::error::Error + Send + Sync + 'static,
    <Cdc as Decoder<StepRequest<Compact>>>::Error: std::error::Error + Send + Sync + 'static,
    SinkT: TaskSink<StepRequest<Compact>, Compact = Compact, Codec = Cdc> + 'static,
{
    type Response = ();
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<(), BoxDynError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<StepRequest<Compact>, Ctx>) -> Self::Future {
        let index = request.args.index;
        let req = request.map(|s| s.step);
        let svc = self.inner.steps.get_mut(&index);
        match svc {
            Some(svc) => {
                let sink = self.sink.clone();
                let fut = svc.call(req).and_then(move |res| async move {
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
            None => match &mut self.inner.fallback {
                Some(fb) => fb(req).boxed(),
                None => ready(Err(BoxDynError::from(StepError::OutOfBound(index)))).boxed(),
            },
        }
    }
}

pub trait StepSinkExt<Step, Ctx, Compact> {
    type Error;

    fn push_step(
        &mut self,
        id: usize,
        value: Step,
    ) -> impl Future<Output = Result<Parts<Ctx>, Self::Error>>;
}

pub trait StartStepSinkExt<Start, Ctx, Compact> {
    type Error;

    fn push_start(&mut self, value: Start)
        -> impl Future<Output = Result<Parts<Ctx>, Self::Error>>;
}

impl<S, T, Compact, Ctx, Err> StepSinkExt<T, Ctx, Compact> for S
where
    S: TaskSink<StepRequest<Compact>, Compact = Compact, Context = Ctx, Error = Err>,
    Err: std::error::Error,
    S::Codec: Encoder<StepRequest<Compact>, Compact = Compact> + Encoder<T, Compact = Compact>,
    Ctx: Default,
    <<S as TaskSink<StepRequest<Compact>>>::Codec as Encoder<T>>::Error: Debug,
{
    type Error = Err;

    async fn push_step(&mut self, id: usize, input: T) -> Result<Parts<Ctx>, Self::Error> {
        let compact = S::Codec::encode(&input).unwrap();
        self.push(StepRequest::new(id, compact)).await
    }
}

impl<S, Compact, Ctx, Err, Input> StartStepSinkExt<Input, Ctx, Compact> for S
where
    S: TaskSink<StepRequest<Compact>, Compact = Compact, Context = Ctx, Error = Err>,
    Err: std::error::Error,
    S::Codec: Encoder<StepRequest<Compact>, Compact = Compact> + Encoder<Input, Compact = Compact>,
    Ctx: Default,
    <<S as TaskSink<StepRequest<Compact>>>::Codec as Encoder<Input>>::Error: Debug,
{
    type Error = Err;

    async fn push_start(&mut self, input: Input) -> Result<Parts<Ctx>, Self::Error> {
        let compact = S::Codec::encode(&input).unwrap();
        self.push(StepRequest::new(0, compact)).await
    }
}

#[cfg(test)]
mod tests {

    use std::{io, str::FromStr};

    use futures::StreamExt;
    use serde_json::Number;
    use tower::util::BoxService;

    use crate::{
        backend::{codec::json::JsonCodec, memory::MemoryStorage},
        request::data::Data,
        service_fn::service_fn,
        worker::{context::WorkerContext, event::Event, ext::event_listener::EventListenerExt},
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
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(3)).await;
                wrk.stop().unwrap();
            });
            println!("{job}");
            Ok(GoTo::Done(()))
        }

        async fn recover<Req: Debug>(req: Req) -> Result<(), BoxDynError> {
            println!("Recovering request: {req:?}");
            Err("Unable to recover".into())
        }

        let steps = StepBuilder::new()
            .step_fn(task1)
            .step_fn(task2)
            .step_fn(task3)
            .fallback(recover);

        let in_memory = MemoryStorage::new_with_json();
        let mut sink = in_memory.sink();
        let _res = sink.push_start(0u32).await.unwrap();

        let _res = sink
            .push(StepRequest::new(
                0,
                serde_json::Value::Number(Number::from_str("1").unwrap()),
            ))
            .await
            .unwrap();

        let _res = sink.push_step(1, ()).await.unwrap();

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .on_event(|ctx, ev| {
                println!("Worker {:?}, On Event = {:?}", ctx.name(), ev);
                if matches!(ev, Event::Error(_)) {
                    ctx.stop().unwrap();
                }
            })
            .build(steps);
        let mut event_stream = worker.stream();
        while let Some(Ok(ev)) = event_stream.next().await {
            println!("On Event = {:?}", ev);
        }
    }
}
