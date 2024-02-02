use std::marker::PhantomData;

use futures::Stream;
use tower::{
    layer::util::{Identity, Stack},
    Layer, Service, ServiceBuilder,
};

use crate::{
    error::Error,
    layers::extensions::Data,
    mq::MessageQueue,
    request::Request,
    service_fn::service_fn,
    service_fn::ServiceFn,
    storage::Storage,
    worker::{Ready, Worker, WorkerId},
    Backend,
};

/// Allows building a [`Worker`].
/// Usually the output is [`Worker<Ready>`]
pub struct WorkerBuilder<Req, Source, Middleware, Serv> {
    id: WorkerId,
    request: PhantomData<Req>,
    layer: ServiceBuilder<Middleware>,
    source: Source,
    service: PhantomData<Serv>,
}

impl<Req, Source, Middleware, Serv> std::fmt::Debug
    for WorkerBuilder<Req, Source, Middleware, Serv>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerBuilder")
            .field("id", &self.id)
            .field("job", &std::any::type_name::<Req>())
            .field("layer", &std::any::type_name::<Middleware>())
            .field("source", &std::any::type_name::<Source>())
            .finish()
    }
}

impl<Serv> WorkerBuilder<(), (), Identity, Serv> {
    /// Build a new [`WorkerBuilder`] instance with a name for the worker to build
    pub fn new<T: AsRef<str>>(name: T) -> WorkerBuilder<(), (), Identity, Serv> {
        let job: PhantomData<()> = PhantomData;
        WorkerBuilder {
            request: job,
            layer: ServiceBuilder::new(),
            source: (),
            id: WorkerId::new(name),
            service: PhantomData,
        }
    }
}

impl<J, S, M, Serv> WorkerBuilder<J, S, M, Serv> {
    /// Consume a stream directly
    pub fn stream<NS: Stream<Item = Result<Option<Request<NJ>>, Error>> + Send + 'static, NJ>(
        self,
        stream: NS,
    ) -> WorkerBuilder<NJ, NS, M, Serv> {
        WorkerBuilder {
            request: PhantomData,
            layer: self.layer,
            source: stream,
            id: self.id,
            service: self.service,
        }
    }

    /// Set the source to a [Storage]
    pub fn with_storage<NS: Storage<Job = NJ>, NJ>(
        self,
        storage: NS,
    ) -> WorkerBuilder<NJ, NS, M, Serv> {
        WorkerBuilder {
            request: PhantomData,
            layer: self.layer,
            source: storage,
            id: self.id,
            service: self.service,
        }
    }

    /// Set the source to a [MessageQueue]
    pub fn with_mq<NS: MessageQueue<NJ>, NJ>(
        self,
        message_queue: NS,
    ) -> WorkerBuilder<NJ, NS, M, Serv> {
        WorkerBuilder {
            request: PhantomData,
            layer: self.layer,
            source: message_queue,
            id: self.id,
            service: self.service,
        }
    }

    /// Set the source to a generic backend that implements only [Backend]
    pub fn source<NS: Backend<Request<NJ>>, NJ>(
        self,
        backend: NS,
    ) -> WorkerBuilder<NJ, NS, M, Serv> {
        WorkerBuilder {
            request: PhantomData,
            layer: self.layer,
            source: backend,
            id: self.id,
            service: self.service,
        }
    }
}

impl<Request, Stream, M, Serv> WorkerBuilder<Request, Stream, M, Serv> {
    /// Allows of decorating the service that consumes jobs.
    /// Allows adding multiple [`tower`] middleware
    pub fn chain<NewLayer>(
        self,
        f: impl Fn(ServiceBuilder<M>) -> ServiceBuilder<NewLayer>,
    ) -> WorkerBuilder<Request, Stream, NewLayer, Serv> {
        let middleware = f(self.layer);

        WorkerBuilder {
            request: self.request,
            layer: middleware,
            id: self.id,
            source: self.source,
            service: self.service,
        }
    }
    /// Allows adding a single layer [tower] middleware
    pub fn layer<U>(self, layer: U) -> WorkerBuilder<Request, Stream, Stack<U, M>, Serv>
    where
        M: Layer<U>,
    {
        WorkerBuilder {
            request: self.request,
            source: self.source,
            layer: self.layer.layer(layer),
            id: self.id,
            service: self.service,
        }
    }

    /// Adds data to the context
    /// This will be shared by all requests
    pub fn data<D>(self, data: D) -> WorkerBuilder<Request, Stream, Stack<Data<D>, M>, Serv>
    where
        M: Layer<Data<D>>,
    {
        WorkerBuilder {
            request: self.request,
            source: self.source,
            layer: self.layer.layer(Data::new(data)),
            id: self.id,
            service: self.service,
        }
    }
}

impl<Req: Send + 'static + Sync, P: Backend<Request<Req>> + 'static, M: 'static, S>
    WorkerFactory<Req, S> for WorkerBuilder<Req, P, M, S>
where
    S: Service<Request<Req>> + Send + 'static + Clone + Sync,
    S::Future: Send,

    S::Response: 'static,
    P::Layer: Layer<S>,
    M: Layer<<P::Layer as Layer<S>>::Service>,
{
    type Source = P;

    type Service = M::Service;
    /// Build a worker, given a tower service
    fn build(self, service: S) -> Worker<Ready<Self::Service, P>> {
        let worker_id = self.id;
        let common_layer = self.source.common_layer(worker_id.clone());
        let poller = self.source;
        let middleware = self.layer.layer(common_layer);
        let service = middleware.service(service);

        Worker::new(worker_id, Ready::new(service, poller))
    }
}

/// Helper trait for building new Workers from [`WorkerBuilder`]
pub trait WorkerFactory<J, S> {
    /// The request source for the worker
    type Source;

    /// The service that the worker will run jobs against
    type Service;
    /// Builds a [`WorkerFactory`] using a [`tower`] service
    /// that can be used to generate a new [`Worker`] using the `build` method
    /// # Arguments
    ///
    /// * `service` - A tower service
    ///
    /// # Examples
    ///
    fn build(self, service: S) -> Worker<Ready<Self::Service, Self::Source>>;
}

/// Helper trait for building new Workers from [`WorkerBuilder`]

pub trait WorkerFactoryFn<J, F, K> {
    /// The request source for the [`Worker`]
    type Source;

    /// The service that the worker will run jobs against
    type Service;
    /// Builds a [`WorkerFactoryFn`] using [`ServiceFn`]
    /// that can be used to generate new [`Worker`] using the `build_fn` method
    /// # Arguments
    ///
    /// * `f` - A functional service.
    ///
    /// # Examples
    ///
    /// A function can take many forms to allow flexibility
    /// - An async function with a single argument of the item being processed
    /// - An async function with an argument of the item being processed plus up-to 16 arguments that are extracted from the request [`Data`]
    ///
    /// A function can return:
    /// - Unit
    /// - primitive
    /// - Result<T, E: Error>
    /// - impl IntoResponse
    ///
    /// ```rust
    /// #[derive(Debug)]
    /// struct Email;
    /// #[derive(Debug)]
    /// struct PgPool;
    /// # struct PgError;
    ///
    /// async fn send_email(email: Email) {
    ///     // Implementation of the job function
    ///     // ...
    /// }
    ///
    /// async fn send_email(email: Email, data: Data<PgPool>) -> Result<(), PgError> {
    ///     // Implementation of the job function?
    ///     // ...
    ///     Ok(())
    /// }
    /// ```
    ///
    fn build_fn(self, f: F) -> Worker<Ready<Self::Service, Self::Source>>;
}

impl<J, W, F, K> WorkerFactoryFn<J, F, K> for W
where
    W: WorkerFactory<J, ServiceFn<F, K>>,
{
    type Source = W::Source;

    type Service = W::Service;

    fn build_fn(self, f: F) -> Worker<Ready<Self::Service, Self::Source>> {
        self.build(service_fn(f))
    }
}
