use crate::backend::Backend;
use crate::builder::WorkerBuilder;
use crate::codec::Codec;
use crate::error::{BoxDynError, Error};
use crate::request::{Parts, Request};
use crate::service_fn::{service_fn, ServiceFn};
use crate::storage::Storage;
use crate::worker::{Ready, Worker};
use futures::future::BoxFuture;
use futures::FutureExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tower::Layer;
use tower::Service;

type BoxedService<Input, Output> = tower::util::BoxService<Input, Output, crate::error::Error>;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum GoTo<N = ()> {
    Next(N),
    Delay { next: N, delay: Duration },
    Done(N),
}

pub struct StepBuilder<Ctx, Compact, Input, Current, Encode> {
    steps: Vec<BoxedService<Request<StepRequest<Compact>, Ctx>, GoTo<Compact>>>,
    current: PhantomData<Current>,
    codec: PhantomData<Encode>,
    input: PhantomData<Input>,
}

impl<Ctx, Compact, Input, Encode> StepBuilder<Ctx, Compact, Input, Input, Encode> {
    pub fn new() -> Self {
        Self {
            steps: Vec::new(),
            current: PhantomData,
            codec: PhantomData,
            input: PhantomData,
        }
    }
}

impl<Ctx, Compact, Input, Current, Encode> StepBuilder<Ctx, Compact, Input, Current, Encode> {
    pub fn build<S>(self, store: S) -> StepService<Ctx, Compact, S> {
        StepService {
            inner: self.steps,
            storage: store,
        }
    }
}

pub struct StepService<Ctx, Compact, S> {
    inner: Vec<BoxedService<Request<StepRequest<Compact>, Ctx>, GoTo<Compact>>>,
    storage: S,
}

impl<Ctx, Compact, S: Storage<Job = StepRequest<Compact>> + Send + Clone + 'static>
    Service<Request<StepRequest<Compact>, Ctx>> for StepService<Ctx, Compact, S>
where
    Compact: DeserializeOwned + Send + Clone + 'static,
    S::Error: Send + Sync + std::error::Error,
{
    type Response = GoTo<Compact>;
    type Error = crate::error::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<StepRequest<Compact>, Ctx>) -> Self::Future {
        let index = req.args.current;

        let service = self
            .inner
            .get_mut(index)
            .expect("Invalid index in inner services");

        // Call the service and save the result to the store.
        let fut = service.call(req);
        let mut storage = self.storage.clone();
        Box::pin(async move {
            match fut.await {
                Ok(response) => {
                    match &response {
                        GoTo::Next(resp) => {
                            storage
                                .push(StepRequest {
                                    current: index + 1,
                                    inner: resp.clone(),
                                })
                                .await
                                .map_err(|e| Error::SourceError(Arc::new(e.into())))?;
                        }
                        GoTo::Delay { next, delay } => {
                            storage
                                .schedule(
                                    StepRequest {
                                        current: index + 1,
                                        inner: next.clone(),
                                    },
                                    delay.as_secs().try_into().unwrap(),
                                )
                                .await
                                .map_err(|e| Error::SourceError(Arc::new(e.into())))?;
                        }
                        GoTo::Done(_) => {
                            // Ignore
                        }
                    };
                    Ok(response)
                }
                Err(e) => Err(e),
            }
        })
    }
}

pub struct TransformingService<S, Compact, Codec, Input, Output> {
    inner: S,
    _req: PhantomData<Compact>,
    _input: PhantomData<Input>,
    _codec: PhantomData<Codec>,
    _output: PhantomData<Output>,
}

impl<S, Compact, Codec, Input, Output> TransformingService<S, Compact, Codec, Input, Output> {
    pub fn new(inner: S) -> Self {
        TransformingService {
            inner,
            _req: PhantomData,
            _input: PhantomData,
            _output: PhantomData,
            _codec: PhantomData,
        }
    }
}

impl<S, Ctx, Input, Output, Compact, Encode> Service<Request<StepRequest<Compact>, Ctx>>
    for TransformingService<S, Compact, Encode, Input, Output>
where
    S: Service<Request<Input, Ctx>, Response = GoTo<Output>>,
    Ctx: Default,
    S::Future: Send + 'static,
    Input: DeserializeOwned,
    Output: Serialize,
    Encode: Codec<Compact = Compact>,
    Encode::Error: Debug,
{
    type Response = GoTo<Compact>;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<StepRequest<Compact>, Ctx>) -> Self::Future {
        let transformed_req: Request<Input, Ctx> =
            { Request::new_with_parts(Encode::decode(req.args.inner).unwrap(), req.parts) };
        let fut = self.inner.call(transformed_req).map(|res| match res {
            Ok(o) => Ok(match o {
                GoTo::Next(next) => GoTo::Next(Encode::encode(next).unwrap()),
                GoTo::Delay { next, delay } => GoTo::Delay {
                    next: Encode::encode(next).unwrap(),
                    delay,
                },
                GoTo::Done(res) => GoTo::Done(Encode::encode(res).unwrap()),
            }),
            Err(e) => Err(e),
        });

        Box::pin(fut)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StepRequest<T> {
    pub inner: T,
    pub current: usize,
}

pub trait Step<Compact, Ctx, S, Input, Current, Next, Encode> {
    fn step(self, service: S) -> StepBuilder<Ctx, Compact, Input, Next, Encode>;
}

impl<S, Ctx, Input, Current, Next, Compact, Encode>
    Step<Compact, Ctx, S, Input, Current, Next, Encode>
    for StepBuilder<Ctx, Compact, Input, Current, Encode>
where
    S: Service<Request<Current, Ctx>, Response = GoTo<Next>, Error = crate::error::Error>
        + Send
        + 'static
        + Sync,
    S::Future: Send + 'static,
    Current: DeserializeOwned + Send + 'static,
    S::Response: 'static,
    Input: Send + 'static + Serialize,
    Ctx: Default + Send,
    Next: 'static + Send + Serialize,
    Compact: Send + 'static,
    Encode: Codec<Compact = Compact> + Send + 'static,
    Encode::Error: Debug,
{
    fn step(mut self, service: S) -> StepBuilder<Ctx, Compact, Input, Next, Encode> {
        self.steps.push(BoxedService::new(TransformingService::<
            S,
            Compact,
            Encode,
            Current,
            Next,
        >::new(service)));
        StepBuilder {
            steps: self.steps,
            current: PhantomData,
            codec: PhantomData,
            input: PhantomData,
        }
    }
}

/// Helper trait for building new Workers from [`WorkerBuilder`]
pub trait StepFn<Compact, Ctx, F, FnArgs, Input, Current, Next, Codec> {
    fn step_fn(self, f: F) -> StepBuilder<Ctx, Compact, Input, Next, Codec>;
}

impl<
        S,
        Ctx: Send + Sync,
        F: Send + Sync,
        FnArgs: Send + Sync,
        Input,
        Current,
        Next,
        Compact,
        Encode,
    > StepFn<Compact, Ctx, F, FnArgs, Input, Current, Next, Encode> for S
where
    S: Step<Compact, Ctx, ServiceFn<F, Current, Ctx, FnArgs>, Input, Current, Next, Encode>,
{
    fn step_fn(self, f: F) -> StepBuilder<Ctx, Compact, Input, Next, Encode> {
        self.step(service_fn(f))
    }
}

/// Helper trait for building new Workers from [`WorkerBuilder`]
pub trait StepWorkerFactory<Ctx, Compact, Input, Output> {
    /// The request source for the worker
    type Source;

    /// The service that the worker will run jobs against
    type Service;

    type Codec;
    /// Builds a [`WorkerFactory`] using a [`tower`] service
    /// that can be used to generate a new [`Worker`] using the `build` method
    /// # Arguments
    ///
    /// * `service` - A tower service
    ///
    /// # Examples
    ///
    fn build_stepped(
        self,
        builder: StepBuilder<Ctx, Compact, Input, Output, Self::Codec>,
    ) -> Worker<Ready<Self::Service, Self::Source>>;
}

impl<P, M, Compact, Ctx, Input, Output> StepWorkerFactory<Ctx, Compact, Input, Output>
    for WorkerBuilder<StepRequest<Compact>, Ctx, P, M, StepService<Ctx, Compact, P>>
where
    M: Layer<StepService<Ctx, Compact, P>>,
    Compact: Send + 'static + Sync,
    P: Backend<Request<StepRequest<Compact>, Ctx>> + 'static,
    P: Storage + Clone,
    M: 'static,
{
    type Source = P;

    type Service = M::Service;

    type Codec = <P as Backend<Request<StepRequest<Compact>, Ctx>>>::Codec;

    fn build_stepped(
        self,
        builder: StepBuilder<Ctx, Compact, Input, Output, Self::Codec>,
    ) -> Worker<Ready<M::Service, P>> {
        let worker_id = self.id;
        let poller = self.source;
        let middleware = self.layer;
        let service = builder.build(poller.clone());
        let service = middleware.service(service);

        Worker::new(worker_id, Ready::new(service, poller))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StepError {
    #[error("CodecError: {0}")]
    CodecError(BoxDynError),
    #[error("StorageError: {0}")]
    StorageError(BoxDynError),
}

pub trait StorageStep<S: Storage, Codec, Compact, Input> {
    async fn push_step<T: Serialize>(
        &mut self,
        step: &StepRequest<T>,
    ) -> Result<Parts<S::Context>, StepError>;

    async fn start_stepped(&mut self, step: Input) -> Result<Parts<S::Context>, StepError>
    where
        Input: Serialize,
    {
        self.push_step(&StepRequest {
            inner: step,
            current: 0,
        })
        .await
    }
}

impl<S, Encode, Compact, Input> StorageStep<S, Encode, Compact, Input> for S
where
    S: Storage<Job = StepRequest<Compact>, Codec = Encode>
        + Backend<Request<StepRequest<Compact>, <S as Storage>::Context>>,
    Encode: Codec<Compact = Compact>,
    Encode::Error: std::error::Error + Send + Sync + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    async fn push_step<T: Serialize>(
        &mut self,
        step: &StepRequest<T>,
    ) -> Result<Parts<S::Context>, StepError> {
        self.push(StepRequest {
            current: step.current,
            inner: Encode::encode(&step.inner).map_err(|e| StepError::CodecError(Box::new(e)))?,
        })
        .await
        .map_err(|e| StepError::StorageError(Box::new(e)))
    }
}
