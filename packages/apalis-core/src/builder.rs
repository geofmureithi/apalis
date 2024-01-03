use std::{error::Error, fmt::Debug, marker::PhantomData};

use futures::Stream;
use tower::{
    layer::util::{Identity, Stack},
    Layer, Service, ServiceBuilder,
};

use crate::{
    context::HasJobContext,
    job::Job,
    job_fn::{job_fn, JobFn},
    request::JobRequest,
    worker::{ready::ReadyWorker, HeartBeat, Worker, WorkerId},
};

/// An abstract that allows building a [`Worker`].
/// Usually the output is [`ReadyWorker`] but you can implement your own via [`WorkerFactory`]
pub struct WorkerBuilder<Job, Source, Middleware> {
    pub(crate) id: WorkerId,
    pub(crate) job: PhantomData<Job>,
    pub(crate) layer: ServiceBuilder<Middleware>,
    pub(crate) source: Source,
    pub(crate) beats: Vec<Box<dyn HeartBeat + Send>>,
    pub(crate) max_concurrent_jobs: usize,
}

impl<Job, Source, Middleware> std::fmt::Debug for WorkerBuilder<Job, Source, Middleware> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerBuilder")
            .field("id", &self.id)
            .field("job", &std::any::type_name::<Job>())
            .field("layer", &std::any::type_name::<Middleware>())
            .field("source", &std::any::type_name::<Source>())
            .field("beats", &self.beats.len())
            .finish()
    }
}

impl WorkerBuilder<(), (), Identity> {
    /// Build a new [`WorkerBuilder`] instance with a name for the worker to build
    pub fn new<T: AsRef<str>>(name: T) -> WorkerBuilder<(), (), Identity> {
        let job: PhantomData<()> = PhantomData;
        WorkerBuilder {
            job,
            layer: ServiceBuilder::new(),
            source: (),
            id: WorkerId::new(name),
            beats: Vec::new(),
            max_concurrent_jobs: 1000,
        }
    }
}

impl<J, S, M> WorkerBuilder<J, S, M> {
    /// Consume a stream directly
    pub fn stream<NS: Stream<Item = Result<Option<JobRequest<NJ>>, E>>, E, NJ>(
        self,
        stream: NS,
    ) -> WorkerBuilder<NJ, NS, M> {
        WorkerBuilder {
            job: PhantomData,
            layer: self.layer,
            source: stream,
            id: self.id,
            beats: self.beats,
            max_concurrent_jobs: self.max_concurrent_jobs,
        }
    }

    /// Get the [`WorkerId`] and build a stream.
    /// Useful when you want to know what worker is consuming the stream.
    pub fn with_stream<
        NS: Fn(&WorkerId) -> ST,
        NJ,
        E,
        ST: Stream<Item = Result<Option<JobRequest<NJ>>, E>>,
    >(
        self,
        stream: NS,
    ) -> WorkerBuilder<NJ, ST, M> {
        WorkerBuilder {
            job: PhantomData,
            layer: self.layer,
            source: stream(&self.id),
            id: self.id,
            beats: self.beats,
            max_concurrent_jobs: self.max_concurrent_jobs,
        }
    }
}

impl<Job, Stream, Serv> WorkerBuilder<Job, Stream, Serv> {
    /// Allows of decorating the service that consumes jobs.
    /// Allows adding multiple [`tower`] middleware
    pub fn middleware<NewService>(
        self,
        f: impl Fn(ServiceBuilder<Serv>) -> ServiceBuilder<NewService>,
    ) -> WorkerBuilder<Job, Stream, NewService> {
        let middleware = f(self.layer);

        WorkerBuilder {
            job: self.job,
            layer: middleware,
            id: self.id,
            source: self.source,
            beats: self.beats,
            max_concurrent_jobs: self.max_concurrent_jobs,
        }
    }
    /// Shorthand for decoration. Allows adding a single layer [tower] middleware
    pub fn layer<U>(self, layer: U) -> WorkerBuilder<Job, Stream, Stack<U, Serv>>
    where
        Serv: Layer<U>,
    {
        WorkerBuilder {
            job: self.job,
            source: self.source,
            layer: self.layer.layer(layer),
            id: self.id,
            beats: self.beats,
            max_concurrent_jobs: self.max_concurrent_jobs,
        }
    }
}

impl<J, S, M, Ser, E, Request> WorkerFactory<J, Ser> for WorkerBuilder<J, S, M>
where
    S: Stream<Item = Result<Option<Request>, E>> + Send + 'static + Unpin,
    J: Job + Send + 'static,
    M: Layer<Ser>,
    <M as Layer<Ser>>::Service: Service<Request> + Send + 'static,
    E: Sync + Send + 'static + Error,
    Request: Send + HasJobContext,
    <<M as Layer<Ser>>::Service as Service<Request>>::Future: std::marker::Send,
    Ser: Service<Request>,
    <Ser as Service<Request>>::Error: Debug,
    <<M as Layer<Ser>>::Service as Service<Request>>::Error: std::fmt::Debug,
    <<M as Layer<Ser>>::Service as Service<Request>>::Future: 'static,
{
    type Worker = ReadyWorker<S, <M as Layer<Ser>>::Service>;
    /// Convert a worker builder to a worker ready to consume jobs
    fn build(self, service: Ser) -> ReadyWorker<S, <M as Layer<Ser>>::Service> {
        ReadyWorker {
            id: self.id,
            stream: self.source,
            service: self.layer.service(service),
            beats: self.beats,
            max_concurrent_jobs: self.max_concurrent_jobs,
        }
    }
}

/// Helper trait for building new Workers from [`WorkerBuilder`]
pub trait WorkerFactory<J, S> {
    /// The worker to build
    type Worker: Worker<J>;
    /// Builds a [`WorkerFactory`] using a [`tower`] service
    /// that can be used to generate new [`Worker`] actors using the `build` method
    /// # Arguments
    ///
    /// * `service` - A tower service
    ///
    /// # Examples
    ///
    fn build(self, service: S) -> Self::Worker;
}

/// Helper trait for building new Workers from [`WorkerBuilder`]

pub trait WorkerFactoryFn<J, F> {
    /// The worker build
    type Worker: Worker<J>;
    /// Builds a [`WorkerFactoryFn`] using a [`crate::job_fn::JobFn`] service
    /// that can be used to generate new [`Worker`] actors using the `build` method
    /// # Arguments
    ///
    /// * `f` - A tower functional service
    ///
    /// # Examples
    ///
    fn build_fn(self, f: F) -> Self::Worker;
}

impl<J, W, F> WorkerFactoryFn<J, F> for W
where
    W: WorkerFactory<J, JobFn<F>>,
{
    type Worker = W::Worker;

    fn build_fn(self, f: F) -> Self::Worker {
        self.build(job_fn(f))
    }
}
