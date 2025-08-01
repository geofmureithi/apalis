//! # Stepped Workflow
//!
//! A stepped workflow is a sequence of tasks (or steps) that are executed one after another.
//! The output of one step is the input of the next step. This allows for creating complex
//! workflows where each step is a separate unit of work.
//!
//! ## Key Concepts
//!
//! - **`Step`**: A single unit of work in a workflow. It is represented as a `tower::Service`
//!   that takes a `Request<T, Ctx>` and returns a `Result<GoTo<O>, E>`, where `T` is the input
//!   type, `O` is the output type, `Ctx` is the context type, and `E` is the error type.
//!
//! - **`GoTo<T>`**: An enum that determines the next action in the workflow.
//!   - `GoTo::Next(T)`: Proceed to the next step with the given output `T`.
//!   - `GoTo::Delay { next: T, delay: Duration }`: Proceed to the next step with output `T` after a specified delay.
//!   - `GoTo::Done(T)`: The workflow is complete. The final value `T` is produced.
//!
//! - **`StepBuilder`**: A builder for constructing a stepped workflow. It allows chaining
//!   steps together using `step` or `step_fn`.
//!
//! - **`StartStepSinkExt::push_start`**: An extension trait for `TaskSink` that allows
//!   starting a new workflow by pushing the initial input.
//!
//! ## Example
//!
//! Here's an example of a three-step workflow.
//!
//! ```rust,ignore
//! use apalis_core::{
//!     prelude::*,
//!     workflow::{
//!         stepped::{GoTo, StepBuilder, StartStepSinkExt},
//!         Workflow,
//!     },
//! };
//! use std::time::Duration;
//!
//! // First step: takes a u32, returns a String
//! async fn step1(input: u32) -> Result<GoTo<String>, std::io::Error> {
//!     println!("Step 1: Received {}", input);
//!     Ok(GoTo::Next(input.to_string()))
//! }
//!
//! // Second step: takes a String, returns a String
//! async fn step2(input: String) -> Result<GoTo<String>, std::io::Error> {
//!     println!("Step 2: Received {}", input);
//!     Ok(GoTo::Delay {
//!         next: format!("Hello, {}!", input),
//!         delay: Duration::from_secs(1),
//!     })
//! }
//!
//! // Third step: takes a String, completes the workflow
//! async fn step3(input: String) -> Result<GoTo<()>, std::io::Error> {
//!     println!("Step 3: Received {}", input);
//!     Ok(GoTo::Done(()))
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let storage = MemoryStorage::new_with_json();
//!     let mut sink = storage.sink();
//!
//!     // Build the workflow
//!     let steps = StepBuilder::new()
//!         .step_fn(step1)
//!         .step_fn(step2)
//!         .step_fn(step3);
//!
//!     // Start the workflow
//!     sink.push_start(123u32).await.unwrap();
//!
//!     // Build and run the worker
//!     let worker = WorkerBuilder::new("stepped-worker")
//!         .backend(storage)
//!         .build(steps);
//!
//!     // Monitor and worker will gracefully shutdown
//!     Monitor::new()
//!         .register(worker)
//!         .run()
//!         .await
//!         .unwrap();
//! }
//! ```
use std::{
    collections::HashMap,
    convert::Infallible,
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
    request::{data::MissingDataError, task_id::TaskId, Parts, Request},
    service_fn::{from_request::FromRequest, service_fn, ServiceFn},
    worker::{
        builder::{ServiceFactory, WorkerBuilder, WorkerFactory},
        Worker,
    },
    workflow::{ComplexBuilder, ComplexThen, GoTo},
};

type BoxedService<Input, Output> = tower::util::BoxService<Input, Output, BoxDynError>;

type SteppedService<Compact, Ctx> = BoxedService<Request<Compact, Ctx>, GoTo<Compact>>;

type RouteService<Compact, Ctx> = BoxedService<Request<StepRequest<Compact>, Ctx>, GoTo<Compact>>;

/// Represents a request for a specific step in a workflow.
///
/// This struct carries the data for a step, its index in the workflow,
/// and the execution context.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct StepRequest<T> {
    /// The index of the step in the workflow.
    pub index: usize,
    /// The data associated with the step.
    pub step: T,
    /// The execution context of the workflow.
    pub execution_ctx: ExecutionContext,
}

impl<T> StepRequest<T> {
    /// Creates a new `StepRequest`.
    pub fn new(index: usize, inner: T, execution_ctx: ExecutionContext) -> Self {
        Self {
            index,
            step: inner,
            execution_ctx,
        }
    }
}

/// A builder for creating stepped workflows.
///
/// `StepBuilder` is used to chain together a sequence of services (steps)
/// to form a workflow.
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
    /// Creates a new `StepBuilder`.
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
    /// Sets a fallback service to be called when a step index is out of bounds.
    ///
    /// This is useful for handling cases where a workflow might attempt to proceed
    /// to a step that doesn't exist, for example, for recovering from errors or
    /// handling dynamic workflows.
    pub fn fallback<F: Future<Output = Result<(), E>> + Send + 'static, E: Into<BoxDynError>>(
        mut self,
        mut fun: impl FnMut(Request<Compact, Ctx>) -> F + Send + Sync + 'static,
    ) -> Self {
        self.fallback = Some(Box::new(move |r| fun(r).map_err(|e| e.into()).boxed()));
        self
    }
}

impl<Input, Current, Compact, Ctx, Codec, R, Next> ComplexThen<R, Next, Compact, Ctx, Codec>
    for StepBuilder<Input, Current, SteppedService<Compact, Ctx>, Codec, Compact, Ctx>
{
    fn then_run(self, service: Next) -> ComplexBuilder<R, Compact, Ctx, Codec> {
        self
    }
}

/// A service that wraps a single step in a workflow, handling encoding and decoding.
pub struct SingleStepService<S, Codec, Current, Next, Compact, Ctx> {
    inner: S,
    _marker: PhantomData<(Codec, Current, Next, Compact, Ctx)>,
}

impl<S, Codec, Current, Next, Compact, Ctx>
    SingleStepService<S, Codec, Current, Next, Compact, Ctx>
{
    /// Creates a new `SingleStepService`.
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
    /// Adds a new step to the workflow.
    ///
    /// Steps are `tower::Service`s that process the job.
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

    /// Adds a new step to the workflow from a function.
    ///
    /// This is a convenience method that wraps a function into a `tower::Service`.
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

impl<Compact, Ctx, Sink, Input, Current, Cdc>
    ServiceFactory<Sink, RouteService<Compact, Ctx>, StepRequest<Compact>, Ctx>
    for StepBuilder<Input, Current, SteppedService<Compact, Ctx>, Cdc, Compact, Ctx>
where
    Input: Send + 'static,
    Current: Send + 'static,
    Cdc: Send
        + 'static
        + Decoder<StepRequest<Compact>, Compact = Compact>
        + Encoder<StepRequest<Compact>, Compact = Compact>,
    <Cdc as Encoder<StepRequest<Compact>>>::Error: std::error::Error + Send + Sync + 'static,
    <Cdc as Decoder<StepRequest<Compact>>>::Error: std::error::Error + Send + Sync + 'static,
    Ctx: 'static,
    Compact: 'static + Send + Clone,
    Sink: TaskSink<StepRequest<Compact>, Codec = Cdc, Compact = Compact, Context = Ctx> + 'static,
    Sink::Error: std::error::Error + Send + Sync,
{
    fn service(self, sink: Sink) -> RouteService<Compact, Ctx> {
        BoxedService::new(RoutedStepService {
            inner: self,
            sink: Arc::new(Mutex::new(sink)),
        })
    }
}

/// Errors that can occur during the execution of a stepped workflow.
#[derive(Debug, thiserror::Error)]
pub enum StepError {
    /// Occurs when a step index is out of bounds.
    #[error("The service at index `{0}` is not available")]
    OutOfBound(usize),
    /// Occurs when encoding or decoding of a step's data fails.
    #[error("Codec failed for step `{0}` expecting type {1}")]
    CodecError(BoxDynError, &'static str),
}

/// A service that routes a `StepRequest` to the appropriate step service.
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
    Compact: Send + 'static + Clone,
    Cdc: Decoder<StepRequest<Compact>, Compact = Compact>
        + Encoder<StepRequest<Compact>, Compact = Compact>,
    <Cdc as Encoder<StepRequest<Compact>>>::Error: std::error::Error + Send + Sync + 'static,
    <Cdc as Decoder<StepRequest<Compact>>>::Error: std::error::Error + Send + Sync + 'static,
    SinkT: TaskSink<StepRequest<Compact>, Compact = Compact, Codec = Cdc> + 'static,
    SinkT::Error: std::error::Error + Send + Sync + 'static,
{
    type Response = GoTo<Compact>;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<GoTo<Compact>, BoxDynError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<StepRequest<Compact>, Ctx>) -> Self::Future {
        let index = request.args.index;
        let task_id = request.parts.task_id.clone();
        let ctx = request.args.execution_ctx.clone();
        let mut req = request.map(|s| s.step);
        req.insert(ctx.clone());
        let svc = self.inner.steps.get_mut(&index);
        match svc {
            Some(svc) => {
                let sink = self.sink.clone();
                let fut = svc.call(req).and_then(move |res| async move {
                    let mut sink = sink.lock().await;
                    let mut ctx = ctx.clone();
                    ctx.previous_nodes.insert(index, task_id);
                    match &res {
                        GoTo::Next(next) => {
                            let req = Request::new(StepRequest::new(index + 1, next.clone(), ctx));
                            let _res = sink.push_request(req).await?;
                        }
                        GoTo::Delay { next, delay } => {
                            let req = Request::new(StepRequest::new(index + 1, next.clone(), ctx));
                            let _res = sink.schedule_request(req, *delay).await?;
                        }
                        GoTo::Done(_) => {}
                    }
                    Ok(res)
                });

                fut.boxed()
            }
            None => ready(Err(BoxDynError::from(StepError::OutOfBound(index)))).boxed(),
        }
    }
}

/// An extension trait for `TaskSink` that allows pushing an intermediate step.
pub trait StepSinkExt<Step, Ctx, Compact> {
    /// The error from the step
    type Error;

    /// Allows pushing an intermediate step to the sink.
    fn push_step(
        &mut self,
        id: usize,
        value: Step,
    ) -> impl Future<Output = Result<Parts<Ctx>, Self::Error>>;
}

/// An extension trait for `TaskSink` that allows starting a workflow.
pub trait StartStepSinkExt<Start, Ctx, Compact> {
    /// The error from the start step
    type Error;

    /// Pushes the starting step of a workflow to the sink.
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
        self.push(StepRequest::new(id, compact, Default::default()))
            .await
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
        self.push(StepRequest::new(0, compact, Default::default()))
            .await
    }
}

/// The execution context for a workflow, containing information about previous steps.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ExecutionContext {
    /// A map of step indices to the `TaskId` of the worker that executed them.
    previous_nodes: HashMap<usize, TaskId>,
}

#[doc(hidden)]
pub fn assert_stepped<B, Input, Current, Svc, Cdc, Compact, Ctx, M>(
    _: &StepBuilder<Input, Current, Svc, Cdc, Compact, Ctx>,
) where
    B: Backend<StepRequest<Compact>, Ctx>,
    M: Layer<RouteService<Compact, Ctx>>,
    Input: Send + 'static,
    Current: Send + 'static,
    Cdc: Send
        + 'static
        + Decoder<StepRequest<Compact>, Compact = Compact>
        + Encoder<StepRequest<Compact>, Compact = Compact>,
    <Cdc as Encoder<StepRequest<Compact>>>::Error: Into<BoxDynError> + Send + Sync + 'static,
    <Cdc as Decoder<StepRequest<Compact>>>::Error: Into<BoxDynError> + Send + Sync + 'static,
    Ctx: 'static,
    Compact: 'static + Send,
    B::Sink:
        TaskSink<StepRequest<Compact>, Codec = Cdc, Compact = Compact, Context = Ctx> + 'static,
    <<B as Backend<StepRequest<Compact>, Ctx>>::Sink as TaskSink<StepRequest<Compact>>>::Error:
        Into<BoxDynError> + Send + Sync,
{
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
        async fn task1(job: u32) -> Result<GoTo<()>, BoxDynError> {
            println!("{job}");
            Ok(GoTo::Next(()))
        }

        async fn task2(_: ()) -> Result<GoTo<usize>, BoxDynError> {
            Ok(GoTo::Next(1))
        }

        async fn task3(
            job: usize,
            wrk: WorkerContext,
            ctx: Data<ExecutionContext>,
        ) -> Result<GoTo<()>, io::Error> {
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(3)).await;
                wrk.stop().unwrap();
            });
            println!("{job}");
            dbg!(&ctx.previous_nodes);
            Ok(GoTo::Done(()))
        }

        async fn recover<Req: Debug>(req: Req) -> Result<(), BoxDynError> {
            println!("Recovering request: {req:?}");
            Err("Unable to recover".into())
        }

        let steps = StepBuilder::new()
            .step_fn(task1)
            .step({
                let svc = ServiceBuilder::new().service(service_fn(task2));
                svc
            })
            .step_fn(task3)
            .fallback(recover);

        let in_memory = MemoryStorage::new_with_json();
        let mut sink = in_memory.sink();
        let _res = sink.push_start(0u32).await.unwrap();

        let _res = sink
            .push(StepRequest::new(
                0,
                serde_json::Value::Number(Number::from_str("1").unwrap()),
                Default::default(),
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
