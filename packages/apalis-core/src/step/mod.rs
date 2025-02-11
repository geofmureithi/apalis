use crate::backend::Backend;
use crate::builder::WorkerBuilder;
use crate::codec::Codec;
use crate::error::Error;
use crate::request::Request;
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
    Done,
}

pub struct StepBuilder<Ctx, Compact, Current, Encode> {
    steps: Vec<BoxedService<Request<StepRequest<Compact>, Ctx>, GoTo<Compact>>>,
    current: PhantomData<Current>,
    codec: PhantomData<Encode>,
}

impl<Ctx, Compact, Current, Encode> StepBuilder<Ctx, Compact, Current, Encode> {
    pub fn new() -> Self {
        Self {
            steps: Vec::new(),
            current: PhantomData,
            codec: PhantomData,
        }
    }

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
                        GoTo::Done => {
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
                GoTo::Done => GoTo::Done,
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

pub trait Step<Compact, Ctx, S, Input, Output, Encode> {
    fn step(self, service: S) -> StepBuilder<Ctx, Compact, Output, Encode>;
}

impl<S, Ctx, Input, Output, Compact, Encode> Step<Compact, Ctx, S, Input, Output, Encode>
    for StepBuilder<Ctx, Compact, Input, Encode>
where
    S: Service<Request<Input, Ctx>, Response = GoTo<Output>, Error = crate::error::Error>
        + Send
        + 'static
        + Sync,
    S::Future: Send + 'static,
    Input: DeserializeOwned,
    S::Response: 'static,
    Input: Send + 'static + Serialize,
    Ctx: Default + Send,
    Output: 'static + Send + Serialize,
    Compact: Send + 'static,
    Encode: Codec<Compact = Compact> + Send + 'static,
    Encode::Error: Debug,
{
    fn step(mut self, service: S) -> StepBuilder<Ctx, Compact, Output, Encode> {
        self.steps.push(BoxedService::new(TransformingService::<
            S,
            Compact,
            Encode,
            Input,
            Output,
        >::new(service)));
        StepBuilder {
            steps: self.steps,
            current: PhantomData,
            codec: PhantomData,
        }
    }
}

/// Helper trait for building new Workers from [`WorkerBuilder`]
pub trait StepFn<Compact, Ctx, F, FnArgs, Input, Output, Codec> {
    fn step_fn(self, f: F) -> StepBuilder<Ctx, Compact, Output, Codec>;
}

impl<S, Ctx: Send + Sync, F: Send + Sync, FnArgs: Send + Sync, Input, Output, Compact, Encode>
    StepFn<Compact, Ctx, F, FnArgs, Input, Output, Encode> for S
where
    S: Step<Compact, Ctx, ServiceFn<F, Input, Ctx, FnArgs>, Input, Output, Encode>,
{
    fn step_fn(self, f: F) -> StepBuilder<Ctx, Compact, Output, Encode> {
        self.step(service_fn(f))
    }
}

/// Helper trait for building new Workers from [`WorkerBuilder`]
pub trait StepWorkerFactory<Req, Ctx, Compact> {
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
    fn build_steps(
        self,
        builder: StepBuilder<Ctx, Compact, (), Self::Codec>,
    ) -> Worker<Ready<Self::Service, Self::Source>>;
}

impl<Req, P, M, Compact, Ctx> StepWorkerFactory<Req, Ctx, Compact>
    for WorkerBuilder<Req, Ctx, P, M, StepService<Ctx, Compact, P>>
where
    M: Layer<StepService<Ctx, Compact, P>>,
    Req: Send + 'static + Sync,
    P: Backend<Request<Req, Ctx>, Compact> + 'static,
    P: Storage + Clone,
    M: 'static,
{
    type Source = P;

    type Service = M::Service;

    type Codec = P::Codec;

    fn build_steps(
        self,
        builder: StepBuilder<Ctx, Compact, (), Self::Codec>,
    ) -> Worker<Ready<M::Service, P>> {
        let worker_id = self.id;
        let poller = self.source;
        let middleware = self.layer;
        let service = builder.build(poller.clone());
        let service = middleware.service(service);

        Worker::new(worker_id, Ready::new(service, poller))
    }
}
