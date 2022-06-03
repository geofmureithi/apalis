use tower::{
    layer::util::{Identity, Stack},
    Layer,
};

#[cfg(feature = "filter")]
#[cfg_attr(docsrs, doc(cfg(feature = "filter")))]
use tower::filter::{AsyncFilterLayer, FilterLayer};

use std::{fmt::Debug, marker::PhantomData};

use crate::{
    job::JobStream,
    job_fn::{job_fn, JobFn},
    worker::Worker,
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
/// ```rust,ignore
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
///     .start().await;
///
/// ```
///
///
/// Defining a middleware stack
///
/// ```rust,ignore
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
///     .start().await;
///
/// ```
#[derive(Debug)]
pub struct WorkerBuilder<T, S, M> {
    job: PhantomData<T>,
    pub(crate) layer: M,
    pub(crate) source: S,
}

impl<S> WorkerBuilder<(), S, Identity> {
    /// Build a new [WorkerBuilder] instance
    pub fn new(source: S) -> WorkerBuilder<S::Job, S, Identity>
    where
        S: JobStream,
    {
        let job: PhantomData<S::Job> = PhantomData;
        WorkerBuilder {
            job,
            layer: Identity::new(),
            source,
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
            source: self.source,
            layer: Stack::new(layer, self.layer),
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
}

/// Helper trait for building new Workers from [WorkerBuilder]
pub trait WorkerFactory<S> {
    /// The worker to build
    type Worker: Worker;
    /// Builds a [WorkerFactory] using a [tower] service
    /// that can be used to generate new [Worker] actors using the `build` method
    /// # Arguments
    ///
    /// * `service` - A tower service
    ///
    /// # Examples
    ///
    fn build(self, service: S) -> Self::Worker;
}

/// Helper trait for building new Workers from [WorkerBuilder]

pub trait WorkerFactoryFn<F> {
    /// The worker build
    type Worker: Worker;
    /// Builds a [WorkerFactoryFn] using a [crate::job_fn::JobFn] service
    /// that can be used to generate new [Worker] actors using the `build` method
    /// # Arguments
    ///
    /// * `f` - A tower functional service
    ///
    /// # Examples
    ///
    fn build_fn(self, f: F) -> Self::Worker;
}

impl<W, F> WorkerFactoryFn<F> for W
where
    W: WorkerFactory<JobFn<F>>,
{
    type Worker = W::Worker;

    fn build_fn(self, f: F) -> Self::Worker {
        self.build(job_fn(f))
    }
}
