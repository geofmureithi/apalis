use std::marker::PhantomData;

use futures::Stream;
use tower::{
    layer::util::{Identity, Stack},
    Layer, Service, ServiceBuilder,
};

use crate::{
    backend::Backend,
    error::Error,
    layers::extensions::Data,
    request::Request,
    service_fn::service_fn,
    service_fn::ServiceFn,
    worker::{Ready, Worker, WorkerId},
};

/// Allows building a [`Worker`].
/// Usually the output is [`Worker<Ready>`]
pub struct WorkerBuilder<Req, Ctx, Source, Middleware, Serv> {
    pub(crate) id: WorkerId,
    pub(crate) request: PhantomData<Request<Req, Ctx>>,
    pub(crate) layer: ServiceBuilder<Middleware>,
    pub(crate) source: Source,
    service: PhantomData<Serv>,
}

impl<Req, Ctx, Source, Middleware, Serv> std::fmt::Debug
    for WorkerBuilder<Req, Ctx, Source, Middleware, Serv>
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

impl<Serv> WorkerBuilder<(), (), (), Identity, Serv> {
    /// Build a new [`WorkerBuilder`] instance with a name for the worker to build
    pub fn new<T: AsRef<str>>(name: T) -> WorkerBuilder<(), (), (), Identity, Serv> {
        let job: PhantomData<Request<(), ()>> = PhantomData;
        WorkerBuilder {
            request: job,
            layer: ServiceBuilder::new(),
            source: (),
            id: WorkerId::new(name),
            service: PhantomData,
        }
    }
}

impl<M, Serv> WorkerBuilder<(), (), (), M, Serv> {
    /// Consume a stream directly
    #[deprecated(since = "0.6.0", note = "Consider using the `.backend`")]
    pub fn stream<
        NS: Stream<Item = Result<Option<Request<NJ, Ctx>>, Error>> + Send + 'static,
        NJ,
        Ctx,
    >(
        self,
        stream: NS,
    ) -> WorkerBuilder<NJ, Ctx, NS, M, Serv> {
        WorkerBuilder {
            request: PhantomData,
            layer: self.layer,
            source: stream,
            id: self.id,
            service: self.service,
        }
    }

    /// Set the source to a backend that implements [Backend]
    pub fn backend<NB: Backend<Request<NJ, Ctx>>, NJ, Res: Send, Ctx>(
        self,
        backend: NB,
    ) -> WorkerBuilder<NJ, Ctx, NB, M, Serv>
    where
        Serv: Service<Request<NJ, Ctx>, Response = Res>,
    {
        WorkerBuilder {
            request: PhantomData,
            layer: self.layer,
            source: backend,
            id: self.id,
            service: self.service,
        }
    }
}

impl<Req, M, Serv, Ctx> WorkerBuilder<Req, Ctx, (), M, Serv> {
    /// Allows of decorating the service that consumes jobs.
    /// Allows adding multiple [`tower`] middleware
    pub fn chain<NewLayer>(
        self,
        f: impl FnOnce(ServiceBuilder<M>) -> ServiceBuilder<NewLayer>,
    ) -> WorkerBuilder<Req, Ctx, (), NewLayer, Serv> {
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
    pub fn layer<U>(self, layer: U) -> WorkerBuilder<Req, Ctx, (), Stack<U, M>, Serv>
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
    pub fn data<D>(self, data: D) -> WorkerBuilder<Req, Ctx, (), Stack<Data<D>, M>, Serv>
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

impl<Req, P, M, S, Ctx> WorkerFactory<Req, Ctx, S> for WorkerBuilder<Req, Ctx, P, M, S>
where
    S: Service<Request<Req, Ctx>>,
    M: Layer<S>,
    P: Backend<Request<Req, Ctx>>,
{
    type Source = P;

    type Service = M::Service;

    fn build(self, service: S) -> Worker<Ready<M::Service, P>> {
        let worker_id = self.id;
        let poller = self.source;
        let middleware = self.layer;
        let service = middleware.service(service);

        Worker::new(worker_id, Ready::new(service, poller))
    }
}
/// Helper trait for building new Workers from [`WorkerBuilder`]
pub trait WorkerFactory<Req, Ctx, S> {
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
pub trait WorkerFactoryFn<Req, Ctx, F, FnArgs> {
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
    /// - ()
    /// - primitive
    /// - Result<T, E: Error>
    /// - impl IntoResponse
    ///
    /// ```rust
    /// # use apalis_core::layers::extensions::Data;
    /// #[derive(Debug)]
    /// struct Email;
    /// #[derive(Debug)]
    /// struct PgPool;
    ///
    /// async fn send_email(email: Email, data: Data<PgPool>) {
    ///     // Implementation of the task function?
    /// }
    /// ```
    ///
    fn build_fn(self, f: F) -> Worker<Ready<Self::Service, Self::Source>>;
}

impl<Req, W, F, Ctx, FnArgs> WorkerFactoryFn<Req, Ctx, F, FnArgs> for W
where
    W: WorkerFactory<Req, Ctx, ServiceFn<F, Req, Ctx, FnArgs>>,
{
    type Source = W::Source;

    type Service = W::Service;

    fn build_fn(self, f: F) -> Worker<Ready<Self::Service, Self::Source>> {
        self.build(service_fn(f))
    }
}
