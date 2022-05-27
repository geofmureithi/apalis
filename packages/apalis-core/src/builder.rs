use actix::prelude::*;
use futures::Future;
use serde::{de::DeserializeOwned, Serialize};
use tower::{
    layer::util::{Identity, Stack},
    Layer, Service,
};

#[cfg(feature = "filter")]
#[cfg_attr(docsrs, doc(cfg(feature = "filter")))]
use tower::filter::{AsyncFilterLayer, FilterLayer};

use std::{fmt::Debug, marker::PhantomData};

use crate::{
    error::JobError,
    job::Job,
    job_fn::{job_fn, JobFn},
    request::JobRequest,
    response::JobResult,
    storage::Storage,
    worker::{Worker, WorkerConfig},
};

/// Configure and build a [Worker] job service.
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
pub struct WorkerBuilder<T, S, M> {
    job: PhantomData<T>,
    layer: M,
    storage: S,
    config: WorkerConfig,
}

impl<T, S> WorkerBuilder<(), S, Identity>
where
    S: Storage<Output = T>,
{
    /// Build a new queue
    pub fn new(storage: S) -> WorkerBuilder<T, S, Identity> {
        let job: PhantomData<T> = PhantomData;
        WorkerBuilder {
            job,
            layer: Identity::new(),
            storage,
            config: Default::default(),
        }
    }
}

impl<T, S, M> WorkerBuilder<T, S, M> {
    /// Add a new layer `T` into the [WorkerBuilder].
    ///
    /// This wraps the inner service with the service provided by a user-defined
    /// [Layer]. The provided layer must implement the [Layer] trait.
    ///
    pub fn layer<U>(self, layer: U) -> WorkerBuilder<T, S, Stack<U, M>>
    where
        M: Layer<U>,
    {
        WorkerBuilder {
            job: self.job,
            storage: self.storage,
            layer: Stack::new(layer, self.layer),
            config: self.config,
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
    #[cfg(feature = "filter")]
    #[cfg_attr(docsrs, doc(cfg(feature = "filter")))]
    pub fn filter<P>(self, predicate: P) -> WorkerBuilder<T, S, Stack<FilterLayer<P>, M>>
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
    /// [`AsyncFilter`]: tower::filter::
    #[cfg(feature = "filter")]
    pub fn filter_async<P>(self, predicate: P) -> WorkerBuilder<T, S, Stack<AsyncFilterLayer<P>, M>>
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
    ) -> WorkerBuilder<T, S, Stack<tower::util::MapResponseLayer<F>, M>>
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
    pub fn map_err<F>(self, f: F) -> WorkerBuilder<T, S, Stack<tower::util::MapErrLayer<F>, M>>
    where
        M: Layer<tower::util::MapErrLayer<F>>,
    {
        self.layer(tower::util::MapErrLayer::new(f))
    }

    /// Represents a config that dictates how a [Worker] runs
    pub fn config(self, config: WorkerConfig) -> WorkerBuilder<T, S, M> {
        WorkerBuilder {
            job: self.job,
            storage: self.storage,
            layer: self.layer,
            config,
        }
    }
    /// Builds a [WorkerBuilder] using a custom service
    /// that can be used to generate new [Worker] actors using the `Actor::start` method
    pub fn build<B>(self, service: B) -> WorkerBuilder<T, S, M::Service>
    where
        M: Layer<B>,
    {
        WorkerBuilder {
            job: PhantomData,
            layer: self.layer.layer(service),
            storage: self.storage,
            config: self.config,
        }
    }

    /// Builds a [QueueFactory] using a [tower::util::ServiceFn] service
    /// that can be used to generate new [Worker] actors using the `Actor::start` method
    /// # Arguments
    ///
    /// * `f` - A tower functional service
    ///
    /// # Examples
    ///

    pub fn build_fn<F>(self, f: F) -> WorkerBuilder<T, S, M::Service>
    where
        M: Layer<JobFn<F>>,
    {
        WorkerBuilder {
            job: PhantomData,
            layer: self.layer.layer(job_fn(f)),
            storage: self.storage,
            config: self.config,
        }
    }
}

impl<T, S, M> WorkerBuilder<T, S, M> {
    /// Allows you to start a [Worker] that starts consuming the storage immediately
    pub fn start<Fut>(self) -> Addr<Worker<T, S, M>>
    where
        S: Storage<Output = T> + Unpin + Send + 'static,
        T: Job + Serialize + Debug + DeserializeOwned + Send + 'static,
        M: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = Fut>
            + Unpin
            + Send
            + 'static,
        Fut: Future<Output = Result<JobResult, JobError>> + 'static,
    {
        let arb = &Arbiter::new();
        Supervisor::start_in_arbiter(&arb.handle(), |_ctx: &mut Context<Worker<T, S, M>>| {
            Worker::new(self.storage, self.layer).config(self.config)
        })
    }

    /// Allows customization before the starting of a [Worker]
    pub fn start_with<Fut, F>(self, f: F) -> Addr<Worker<T, S, M>>
    where
        S: Storage<Output = T> + Unpin + Send + 'static,
        T: Job + Serialize + Debug + DeserializeOwned + Send + 'static,
        M: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = Fut>
            + Unpin
            + Send
            + 'static,
        Fut: Future<Output = Result<JobResult, JobError>> + 'static,
        F: FnOnce(Self, &mut Context<Worker<T, S, M>>) -> Worker<T, S, M> + Send + 'static,
    {
        let arb = &Arbiter::new();
        Supervisor::start_in_arbiter(&arb.handle(), |ctx| f(self, ctx))
    }
}
