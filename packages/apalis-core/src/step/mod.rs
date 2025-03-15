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
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tower::Layer;
use tower::Service;

type BoxedService<Input, Output> = tower::util::BoxService<Input, Output, crate::error::Error>;

type SteppedService<Compact, Index, Ctx> =
    BoxedService<Request<StepRequest<Compact, Index>, Ctx>, GoTo<Compact>>;

/// Allows control of the next step
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum GoTo<N = ()> {
    /// Go to the next step immediately
    Next(N),
    /// Delay the next step for some time
    Delay {
        /// The input of the next step
        next: N,
        /// The period to delay
        delay: Duration,
    },
    /// Complete execution
    Done(N),
}

/// A type that allows building the steps order
#[derive(Debug)]
pub struct StepBuilder<Ctx, Compact, Input, Current, Encode, Index = usize> {
    steps: HashMap<Index, SteppedService<Compact, Index, Ctx>>,
    current_index: Index,
    current: PhantomData<Current>,
    codec: PhantomData<Encode>,
    input: PhantomData<Input>,
}

impl<Ctx, Compact, Input, Encode, Index: Default> Default
    for StepBuilder<Ctx, Compact, Input, Input, Encode, Index>
{
    fn default() -> Self {
        Self {
            steps: HashMap::new(),
            current_index: Index::default(),
            current: PhantomData,
            codec: PhantomData,
            input: PhantomData,
        }
    }
}

impl<Ctx, Compact, Input, Encode> StepBuilder<Ctx, Compact, Input, Input, Encode, usize> {
    /// Create a new StepBuilder
    pub fn new() -> Self {
        Self {
            steps: HashMap::new(),
            current_index: usize::default(),
            current: PhantomData,
            codec: PhantomData,
            input: PhantomData,
        }
    }

    /// Build a new StepBuilder with a custom stepper
    pub fn new_with_stepper<I: Default>() -> StepBuilder<Ctx, Compact, Input, Input, Encode, I> {
        StepBuilder {
            steps: HashMap::new(),
            current_index: I::default(),
            current: PhantomData,
            codec: PhantomData,
            input: PhantomData,
        }
    }
}

// impl<Ctx, Compact, Input, Encode, Index> StepBuilder<Ctx, Compact, Input, Input, Encode, Index> {
//     pub fn new_with_index<I>() -> Self
//     where
//         Index: Default,
//     {
//         Self {
//             steps: HashMap::new(),
//             current_index: Index::default(),
//             current: PhantomData,
//             codec: PhantomData,
//             input: PhantomData,
//         }
//     }
// }

impl<Ctx, Compact, Input, Current, Encode, Index>
    StepBuilder<Ctx, Compact, Input, Current, Encode, Index>
{
    /// Finalize the step building process
    pub fn build<S>(self, store: S) -> StepService<Ctx, Compact, Input, S, Index> {
        StepService {
            inner: self.steps,
            storage: store,
            input: PhantomData,
        }
    }
}

/// Represents the tower service holding the different steps
#[derive(Debug)]
pub struct StepService<Ctx, Compact, Input, S, Index> {
    inner: HashMap<Index, SteppedService<Compact, Index, Ctx>>,
    storage: S,
    input: PhantomData<Input>,
}

impl<
        Ctx,
        Compact,
        S: Storage<Job = StepRequest<Compact, Index>> + Send + Clone + 'static,
        Input,
        Index,
    > Service<Request<StepRequest<Compact, Index>, Ctx>>
    for StepService<Ctx, Compact, Input, S, Index>
where
    Compact: DeserializeOwned + Send + Clone + 'static,
    S::Error: Send + Sync + std::error::Error,
    Index: StepIndex + Send + Sync + 'static,
{
    type Response = GoTo<Compact>;
    type Error = crate::error::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<StepRequest<Compact, Index>, Ctx>) -> Self::Future {
        let index = &req.args.index;
        let next_index = index.next();

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
                                    index: next_index,
                                    step: resp.clone(),
                                })
                                .await
                                .map_err(|e| Error::SourceError(Arc::new(e.into())))?;
                        }
                        GoTo::Delay { next, delay } => {
                            storage
                                .schedule(
                                    StepRequest {
                                        index: next_index,
                                        step: next.clone(),
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

struct TransformingService<S, Compact, Input, Current, Next, Codec> {
    inner: S,
    _req: PhantomData<Compact>,
    _input: PhantomData<Input>,
    _codec: PhantomData<Codec>,
    _output: PhantomData<Next>,
    _current: PhantomData<Current>,
}

impl<S, Compact, Codec, Input, Current, Next>
    TransformingService<S, Compact, Input, Current, Next, Codec>
{
    fn new(inner: S) -> Self {
        TransformingService {
            inner,
            _req: PhantomData,
            _input: PhantomData,
            _output: PhantomData,
            _codec: PhantomData,
            _current: PhantomData,
        }
    }
}

impl<S, Ctx, Input, Current, Next, Compact, Encode, Index>
    Service<Request<StepRequest<Compact, Index>, Ctx>>
    for TransformingService<S, Compact, Input, Current, Next, Encode>
where
    S: Service<Request<Current, Ctx>, Response = GoTo<Next>>,
    Ctx: Default,
    S::Future: Send + 'static,
    Current: DeserializeOwned,
    Next: Serialize,
    Encode: Codec<Compact = Compact>,
    Encode::Error: Debug,
{
    type Response = GoTo<Compact>;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<StepRequest<Compact, Index>, Ctx>) -> Self::Future {
        let transformed_req: Request<Current, Ctx> = {
            Request::new_with_parts(
                Encode::decode(req.args.step).expect(&format!(
                    "Could not decode step, expecting {}",
                    std::any::type_name::<Current>()
                )),
                req.parts,
            )
        };
        let fut = self.inner.call(transformed_req).map(|res| match res {
            Ok(o) => Ok(match o {
                GoTo::Next(next) => {
                    GoTo::Next(Encode::encode(next).expect("Could not encode the next step"))
                }
                GoTo::Delay { next, delay } => GoTo::Delay {
                    next: Encode::encode(next).expect("Could not encode the next step"),
                    delay,
                },
                GoTo::Done(res) => {
                    GoTo::Done(Encode::encode(res).expect("Could not encode the next step"))
                }
            }),
            Err(e) => Err(e),
        });

        Box::pin(fut)
    }
}

/// Represents a specific step
#[derive(Debug, Serialize, Deserialize)]
pub struct StepRequest<T, Index = usize> {
    step: T,
    index: Index,
}

impl<T, Index> StepRequest<T, Index> {
    /// Build a new step
    pub fn new(step: T) -> Self
    where
        Index: Default,
    {
        Self {
            step,
            index: Index::default(),
        }
    }

    /// Build a new step with a custom index
    pub fn new_with_index(step: T, index: Index) -> Self {
        Self { step, index }
    }
}

/// Helper trait for building new steps from [`StepBuilder`]
pub trait Step<S, Ctx, Compact, Input, Current, Next, Encode, Index> {
    /// Helper function for building new steps from [`StepBuilder`]
    fn step(self, service: S) -> StepBuilder<Ctx, Compact, Input, Next, Encode, Index>;
}

impl<S, Ctx, Input, Current, Next, Compact, Encode, Index>
    Step<S, Ctx, Compact, Input, Current, Next, Encode, Index>
    for StepBuilder<Ctx, Compact, Input, Current, Encode, Index>
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
    Index: StepIndex,
{
    fn step(mut self, service: S) -> StepBuilder<Ctx, Compact, Input, Next, Encode, Index> {
        let next = self.current_index.next();
        self.steps.insert(
            self.current_index,
            BoxedService::new(TransformingService::<
                S,
                Compact,
                Input,
                Current,
                Next,
                Encode,
            >::new(service)),
        );
        StepBuilder {
            steps: self.steps,
            current: PhantomData,
            codec: PhantomData,
            input: PhantomData,
            current_index: next,
        }
    }
}

/// Helper trait for building new steps from [`StepBuilder`]
pub trait StepFn<F, FnArgs, Ctx, Compact, Input, Current, Next, Codec, Index> {
    /// Helper function for building new steps from [`StepBuilder`]
    fn step_fn(self, f: F) -> StepBuilder<Ctx, Compact, Input, Next, Codec, Index>;
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
        Index,
    > StepFn<F, FnArgs, Ctx, Compact, Input, Current, Next, Encode, Index> for S
where
    S: Step<ServiceFn<F, Current, Ctx, FnArgs>, Ctx, Compact, Input, Current, Next, Encode, Index>,
{
    fn step_fn(self, f: F) -> StepBuilder<Ctx, Compact, Input, Next, Encode, Index> {
        self.step(service_fn(f))
    }
}

/// Helper trait for building new Workers from [`WorkerBuilder`]
pub trait StepWorkerFactory<Ctx, Compact, Input, Output, Index> {
    /// The request source for the worker
    type Source;

    /// The service that the worker will run jobs against
    type Service;

    /// Represents the codec for the backend bound
    type Codec;
    /// Builds a [`StepWorkerFactory`] using a [`tower`] service
    /// that can be used to generate a new [`Worker`] using the `build_stepped` method
    /// # Arguments
    ///
    /// * `service` - A tower service
    ///
    /// # Examples
    ///
    fn build_stepped(
        self,
        builder: StepBuilder<Ctx, Compact, Input, Output, Self::Codec, Index>,
    ) -> Worker<Ready<Self::Service, Self::Source>>;
}

impl<Req, P, M, Ctx, Input, Compact, Output, Index>
    StepWorkerFactory<Ctx, Compact, Input, Output, Index>
    for WorkerBuilder<Req, Ctx, P, M, StepService<Ctx, Compact, Input, P, Index>>
where
    Compact: Send + 'static + Sync,
    P: Backend<Request<StepRequest<Compact, Index>, Ctx>> + 'static,
    P: Storage<Job = StepRequest<Compact, Index>> + Clone,
    M: Layer<StepService<Ctx, Compact, Input, P, Index>> + 'static,
{
    type Source = P;

    type Service = M::Service;

    type Codec = <P as Backend<Request<StepRequest<Compact, Index>, Ctx>>>::Codec;

    fn build_stepped(
        self,
        builder: StepBuilder<Ctx, Compact, Input, Output, Self::Codec, Index>,
    ) -> Worker<Ready<M::Service, P>> {
        let worker_id = self.id;
        let poller = self.source;
        let middleware = self.layer;
        let service = builder.build(poller.clone());
        let service = middleware.service(service);

        Worker::new(worker_id, Ready::new(service, poller))
    }
}

/// Errors encountered while stepping through jobs
#[derive(Debug, thiserror::Error)]
pub enum StepError {
    /// Encountered an encoding error
    #[error("CodecError: {0}")]
    CodecError(BoxDynError),
    /// Encountered an error while pushing to the storage
    #[error("StorageError: {0}")]
    StorageError(BoxDynError),
}

/// Helper trait that transforms a storage with stepping capability
pub trait SteppableStorage<S: Storage, Codec, Compact, Input, Index> {
    /// Push a step with a custom index
    fn push_step<T: Serialize + Send>(
        &mut self,
        step: StepRequest<T, Index>,
    ) -> impl Future<Output = Result<Parts<S::Context>, StepError>> + Send;

    /// Push the first step
    fn start_stepped(
        &mut self,
        step: Input,
    ) -> impl Future<Output = Result<Parts<S::Context>, StepError>> + Send
    where
        Input: Serialize + Send,
        Index: Default,
        Self: Send,
    {
        async {
            self.push_step(StepRequest {
                step,
                index: Index::default(),
            })
            .await
        }
    }
}

impl<S, Encode, Compact, Input, Index> SteppableStorage<S, Encode, Compact, Input, Index> for S
where
    S: Storage<Job = StepRequest<Compact, Index>, Codec = Encode>
        + Backend<Request<StepRequest<Compact, Index>, <S as Storage>::Context>>
        + Send,
    Encode: Codec<Compact = Compact>,
    Encode::Error: std::error::Error + Send + Sync + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
    Compact: Send,
    Index: Send,
{
    async fn push_step<T: Serialize + Send>(
        &mut self,
        step: StepRequest<T, Index>,
    ) -> Result<Parts<S::Context>, StepError> {
        self.push(StepRequest {
            index: step.index,
            step: Encode::encode(&step.step).map_err(|e| StepError::CodecError(Box::new(e)))?,
        })
        .await
        .map_err(|e| StepError::StorageError(Box::new(e)))
    }
}

/// A helper trait for planning the step index
/// TODO: This will need to be improved to offer more flexibility
pub trait StepIndex: Eq + Hash {
    /// Returns the next item in the index
    fn next(&self) -> Self;
}

impl StepIndex for usize {
    fn next(&self) -> Self {
        *self + 1
    }
}

impl StepIndex for u32 {
    fn next(&self) -> Self {
        *self + 1
    }
}
