use actix::{
    clock::{interval_at, Instant},
    Actor, Addr, Arbiter, AsyncContext, Context, Supervisor,
};
use futures::Future;
use serde::{de::DeserializeOwned, Serialize};
use tower::{
    filter::{AsyncFilterLayer, FilterLayer},
    layer::util::{Identity, Stack},
    Layer, Service, ServiceExt,
};

use std::{collections::HashMap, fmt::Debug, marker::PhantomData, time::Duration};

use crate::{
    error::JobError,
    job::Job,
    job_fn::{job_fn, JobFn},
    request::JobRequest,
    response::JobResult,
    service::JobService,
    storage::Storage,
    streams::{FetchJobStream, HeartbeatStream},
    worker::{DefaultController, Worker, WorkerController, WorkerPulse},
};

/// Configure and build a [Queue] job service.
///
/// `WorkerBuilder` collects all the components and configuration required to
/// build a job service. Once the service is defined, it can be built
/// with `build`.
///
/// # Examples
///
/// Defining a job service with the default [JobService];
///
/// ```rust
///
/// use apalis::WorkerBuilder;
/// use apalis::sqlite::SqliteStorage;
///
/// let sqlite = SqliteStorage::new("sqlite::memory:").await.unwrap();
///
/// async fn email_service(job: JobRequest<Email>) -> Result<JobResult, JobError> {
///    Ok(JobResult::Success)
/// }
///
/// let addr = WorkerBuilder::new(sqlite)
///     .build_fn(email_service)
///     .start();
///
/// ```
///
///
/// Defining a middleware stack
///
/// ```rust
/// use apalis::layers::{
///    extensions::Extension,
///    retry::{JobRetryPolicy, RetryLayer},
/// };
///
/// use apalis::WorkerBuilder;
/// use apalis::sqlite::SqliteStorage;
///
/// let sqlite = SqliteStorage::connect("sqlite::memory:").await.unwrap();
///
/// #[derive(Clone)]
/// struct JobState {}
///
/// let addr = WorkerBuilder::new(sqlite)
///     .layer(RetryLayer::new(JobRetryPolicy))
///     .layer(Extension(JobState {}))
///     .build()
///     .start();
///
/// ```
pub struct WorkerBuilder<T, S, M, C> {
    job: PhantomData<T>,
    layer: M,
    storage: S,
    controller: C,
}

impl<T, S> WorkerBuilder<(), S, Identity, DefaultController>
where
    S: Storage<Output = T>,
{
    /// Build a new queue
    pub fn new(storage: S) -> WorkerBuilder<T, S, Identity, DefaultController> {
        let job: PhantomData<T> = PhantomData;
        WorkerBuilder {
            job,
            layer: Identity::new(),
            storage,
            controller: DefaultController,
        }
    }
}

impl<T, S, M, C> WorkerBuilder<T, S, M, C> {
    /// Add a new layer `T` into the [WorkerBuilder].
    ///
    /// This wraps the inner service with the service provided by a user-defined
    /// [Layer]. The provided layer must implement the [Layer] trait.
    ///
    pub fn layer<U>(self, layer: U) -> WorkerBuilder<T, S, Stack<U, M>, C>
    where
        M: Layer<U>,
    {
        WorkerBuilder {
            job: self.job,
            storage: self.storage,
            layer: Stack::new(layer, self.layer),
            controller: self.controller,
        }
    }

    /// Conditionally reject requests based on `predicate`.
    ///
    /// `predicate` must implement the [`Predicate`] trait.
    ///
    /// This wraps the inner service with an instance of the [`Filter`]
    /// middleware.
    ///
    /// [`Filter`]: tower::filter::Filter
    pub fn filter<P>(self, predicate: P) -> WorkerBuilder<T, S, Stack<FilterLayer<P>, M>, C>
    where
        M: Layer<FilterLayer<P>>,
    {
        self.layer(FilterLayer::new(predicate))
    }

    /// Conditionally reject requests based on an asynchronous `predicate`.
    ///
    /// `predicate` must implement the [`AsyncPredicate`] trait.
    ///
    /// This wraps the inner service with an instance of the [`AsyncFilter`]
    /// middleware.
    /// [`AsyncFilter`]: tower::filter::AsyncFilter
    pub fn filter_async<P>(
        self,
        predicate: P,
    ) -> WorkerBuilder<T, S, Stack<AsyncFilterLayer<P>, M>, C>
    where
        M: Layer<AsyncFilterLayer<P>>,
    {
        self.layer(AsyncFilterLayer::new(predicate))
    }

    /// Map one response type to another.
    ///
    /// This wraps the inner service with an instance of the [`MapResponse`]
    /// middleware.
    ///
    /// See the documentation for the [`map_response` combinator] for details.
    ///
    /// [`MapResponse`]: tower::util::MapResponse
    /// [`map_response` combinator]: tower::util::ServiceExt::map_response
    pub fn map_response<F>(
        self,
        f: F,
    ) -> WorkerBuilder<T, S, Stack<tower::util::MapResponseLayer<F>, M>, C>
    where
        M: Layer<tower::util::MapResponseLayer<F>>,
    {
        self.layer(tower::util::MapResponseLayer::new(f))
    }

    /// Map one error type to another.
    ///
    /// This wraps the inner service with an instance of the [`MapErr`]
    /// middleware.
    ///
    /// See the documentation for the [`map_err` combinator] for details.
    ///
    /// [`MapErr`]: tower::util::MapErr
    /// [`map_err` combinator]: tower::util::ServiceExt::map_err
    pub fn map_err<F>(self, f: F) -> WorkerBuilder<T, S, Stack<tower::util::MapErrLayer<F>, M>, C>
    where
        M: Layer<tower::util::MapErrLayer<F>>,
    {
        self.layer(tower::util::MapErrLayer::new(f))
    }

    /// Represents a controller that dictates how a [Queue] runs
    pub fn controller<W>(mut self, controller: W) -> WorkerBuilder<T, S, M, W> {
        WorkerBuilder {
            job: self.job,
            storage: self.storage,
            layer: self.layer,
            controller,
        }
    }
    /// Builds a [QueueFactory] using the default [JobService] service
    /// that can be used to generate new [Queue] actors using the `Actor::start` method
    pub fn build(self) -> WorkerFactory<T, S, M::Service, C>
    where
        M: Layer<JobService>,
    {
        WorkerFactory {
            job: PhantomData,
            service: self.layer.layer(JobService),
            storage: self.storage,
            controller: self.controller,
        }
    }

    /// Builds a [QueueFactory] using a [tower::util::ServiceFn] service
    /// that can be used to generate new [Queue] actors using the `Actor::start` method
    /// # Arguments
    ///
    /// * `f` - A tower functional service
    ///
    /// # Examples
    ///

    pub fn build_fn<F>(self, f: F) -> WorkerFactory<T, S, M::Service, C>
    where
        M: Layer<JobFn<F>>,
    {
        WorkerFactory {
            job: PhantomData,
            service: self.layer.layer(job_fn(f)),
            storage: self.storage,
            controller: self.controller,
        }
    }
}

/// Represents a factory for [Queue]
pub struct WorkerFactory<T, S, M, C> {
    job: PhantomData<T>,
    /// The [Service] for executing jobs
    pub service: M,
    /// The [Storage] for producing a stream of jobs
    pub storage: S,

    pub controller: C,
}

impl<T, S, M, C> WorkerFactory<T, S, M, C> {
    /// Allows you to start a [Queue] that starts consuming the storage immediately
    pub fn start<Fut>(self) -> Addr<Worker<T, S, M, C>>
    where
        S: Storage<Output = T> + Unpin + Send + 'static,
        T: Job + Serialize + Debug + DeserializeOwned + Send + 'static,
        M: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = Fut>
            + Unpin
            + Send
            + 'static,
        Fut: Future<Output = Result<JobResult, JobError>> + 'static,
        C: WorkerController<T> + Unpin + Send + 'static,
    {
        let arb = &Arbiter::new();
        Supervisor::start_in_arbiter(&arb.handle(), |ctx: &mut Context<Worker<T, S, M, C>>| {
            Worker::new(self.storage, self.service).controller(self.controller)
        })
    }

    /// Allows customization before the starting of a [Queue]
    pub fn start_with<Fut, F>(self, f: F) -> Addr<Worker<T, S, M, C>>
    where
        S: Storage<Output = T> + Unpin + Send + 'static,
        T: Job + Serialize + Debug + DeserializeOwned + Send + 'static,
        M: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = Fut>
            + Unpin
            + Send
            + 'static,
        Fut: Future<Output = Result<JobResult, JobError>> + 'static,
        F: FnOnce(Self, &mut Context<Worker<T, S, M, C>>) -> Worker<T, S, M, C> + Send + 'static,
        C: WorkerController<T> + Unpin + Send + 'static,
    {
        let arb = &Arbiter::new();
        Supervisor::start_in_arbiter(&arb.handle(), |ctx| f(self, ctx))
    }
}
