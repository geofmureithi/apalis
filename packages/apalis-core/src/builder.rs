use actix::{
    clock::{interval_at, Instant},
    Actor, Addr, Arbiter, AsyncContext, Context,
};
use futures::Future;
use serde::{de::DeserializeOwned, Serialize};
use tower::{
    filter::{AsyncFilterLayer, FilterLayer},
    layer::util::{Identity, Stack},
    Layer, Service,
};

use std::{collections::HashMap, fmt::Debug, marker::PhantomData, time::Duration};

use crate::{
    error::JobError,
    queue::{Heartbeat, Queue},
    request::JobRequest,
    response::JobResult,
    service::JobService,
    storage::Storage,
    streams::{FetchJobStream, HeartbeatStream},
};

/// Configure and build a [Queue] job service.
///
/// `QueueBuilder` collects all the components and configuration required to
/// build a job service. Once the service is defined, it can be built
/// with `build`.
///
/// # Examples
///
/// Defining a job service with the default [JobService];
///
/// ```rust
///
/// use apalis::QueueBuilder;
/// use apalis::sqlite::SqliteStorage;
///
/// let sqlite = SqliteStorage::new("sqlite::memory:").await.unwrap();
///
/// async fn email_service(job: JobRequest<Email>) -> Result<JobResult, JobError> {
///    Ok(JobResult::Success)
/// }
///
/// let addr = QueueBuilder::new(sqlite)
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
/// use apalis::QueueBuilder;
/// use apalis::sqlite::SqliteStorage;
///
/// let sqlite = SqliteStorage::new("sqlite::memory:").await.unwrap();
///
/// #[derive(Clone)]
/// struct JobState {}
///
/// let addr = QueueBuilder::new(sqlite)
///     .layer(RetryLayer::new(JobRetryPolicy))
///     .layer(Extension(JobState {}))
///     .build()
///     .start();
///
/// ```
pub struct QueueBuilder<T, S, M> {
    job: PhantomData<T>,
    layer: M,
    storage: S,
    fetch_interval: Duration,
    heartbeats: HashMap<Heartbeat, Duration>,
}

impl<T, S> QueueBuilder<(), S, Identity>
where
    S: Storage<Output = T>,
{
    /// Build a new queue
    pub fn new(storage: S) -> QueueBuilder<T, S, Identity> {
        let job: PhantomData<T> = PhantomData;
        QueueBuilder {
            job,
            layer: Identity::new(),
            storage,
            fetch_interval: Duration::from_millis(50),
            heartbeats: HashMap::new(),
        }
    }
}

impl<T, S, M> QueueBuilder<T, S, M> {
    /// Add a new layer `T` into the [QueueBuilder].
    ///
    /// This wraps the inner service with the service provided by a user-defined
    /// [Layer]. The provided layer must implement the [Layer] trait.
    ///
    pub fn layer<U>(self, layer: U) -> QueueBuilder<T, S, Stack<U, M>>
    where
        M: Layer<U>,
    {
        QueueBuilder {
            job: self.job,
            storage: self.storage,
            layer: Stack::new(layer, self.layer),
            fetch_interval: self.fetch_interval,
            heartbeats: self.heartbeats,
        }
    }

    /// Conditionally reject requests based on `predicate`.
    ///
    /// `predicate` must implement the [`Predicate`] trait.
    ///
    /// This wraps the inner service with an instance of the [`Filter`]
    /// middleware.
    ///
    /// [`Filter`]: crate::filter
    pub fn filter<P>(self, predicate: P) -> QueueBuilder<T, S, Stack<FilterLayer<P>, M>>
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
    pub fn filter_async<P>(self, predicate: P) -> QueueBuilder<T, S, Stack<AsyncFilterLayer<P>, M>>
    where
        M: Layer<AsyncFilterLayer<P>>,
    {
        self.layer(AsyncFilterLayer::new(predicate))
    }

    // fn finalize<K>(self, service: K) -> M::Service
    // where
    //     M: Layer<K>,
    // {
    //     self.layer.layer(service)
    // }

    /// Represents a heartbeat to be sent by a [Queue] to the [Storage].
    pub fn heartbeat(mut self, heartbeat: Heartbeat, duration: Duration) -> Self {
        self.heartbeats.insert(heartbeat, duration);
        QueueBuilder {
            job: self.job,
            storage: self.storage,
            layer: self.layer,
            fetch_interval: duration,
            heartbeats: self.heartbeats,
        }
    }

    /// Represents the fetch interval by a [Queue] from the [Storage].
    /// Recommended 50ms - 100ms
    /// Can be lowered to increase a queue's priority
    pub fn fetch_interval(self, duration: Duration) -> Self {
        QueueBuilder {
            job: self.job,
            storage: self.storage,
            layer: self.layer,
            fetch_interval: duration,
            heartbeats: self.heartbeats,
        }
    }
    /// Builds a [QueueFactory] using the default [JobService] service
    /// that can be used to generate new [Queue] actors using the `Actor::start` method
    pub fn build(self) -> QueueFactory<T, S, M::Service>
    where
        M: Layer<JobService>,
    {
        QueueFactory {
            job: PhantomData,
            service: self.layer.layer(JobService),
            storage: self.storage,
            fetch_interval: self.fetch_interval,
            heartbeats: self.heartbeats,
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

    pub fn build_fn<F>(self, f: F) -> QueueFactory<T, S, M::Service>
    where
        M: Layer<tower::util::ServiceFn<F>>,
    {
        QueueFactory {
            job: PhantomData,
            service: self.layer.layer(::tower::util::service_fn(f)),
            storage: self.storage,
            fetch_interval: self.fetch_interval,
            heartbeats: self.heartbeats,
        }
    }
}

pub struct QueueFactory<T, S, M> {
    job: PhantomData<T>,
    pub service: M,
    pub storage: S,
    fetch_interval: Duration,
    heartbeats: HashMap<Heartbeat, Duration>,
}

impl<T, S, M> QueueFactory<T, S, M> {
    /// Allows you to start a [Queue] that starts consuming the storage immediately
    pub fn start<Fut>(self) -> Addr<Queue<T, S, M>>
    where
        S: Storage<Output = T> + Unpin + Send + 'static,
        T: Serialize + Debug + DeserializeOwned + Send + 'static,
        M: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = Fut>
            + Unpin
            + Send
            + 'static,
        Fut: Future<Output = Result<JobResult, JobError>> + 'static,
    {
        let arb = &Arbiter::new();
        Actor::start_in_arbiter(&arb.handle(), |ctx: &mut Context<Queue<T, S, M>>| {
            let start = Instant::now() + Duration::from_millis(5);
            // ctx.add_stream(FetchJobStream::new(interval_at(start, self.fetch_interval)));
            for (heartbeat, duration) in self.heartbeats.into_iter() {
                ctx.add_stream(HeartbeatStream::new(
                    heartbeat,
                    interval_at(start, duration),
                ));
            }
            Queue::new(self.storage, self.service)
        })
    }

    /// Allows customization before the starting of a [Queue]
    pub fn start_with<Fut, F>(self, f: F) -> Addr<Queue<T, S, M>>
    where
        S: Storage<Output = T> + Unpin + Send + 'static,
        T: Serialize + Debug + DeserializeOwned + Send + 'static,
        M: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = Fut>
            + Unpin
            + Send
            + 'static,
        Fut: Future<Output = Result<JobResult, JobError>> + 'static,
        F: FnOnce(Self, &mut Context<Queue<T, S, M>>) -> Queue<T, S, M> + Send + 'static,
    {
        let arb = &Arbiter::new();
        Actor::start_in_arbiter(&arb.handle(), |ctx| f(self, ctx))
    }
}
