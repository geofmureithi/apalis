use crate::codec::Codec;
use crate::error::{BoxDynError, Error};
use crate::request::Request;
use crate::response::Response;
use futures::channel::mpsc::{SendError, Sender};
use futures::SinkExt;
use futures::{future::BoxFuture, Future, FutureExt};
use serde::Serialize;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::{fmt, sync::Arc};
pub use tower::{
    layer::layer_fn, layer::util::Identity, util::BoxCloneService, Layer, Service, ServiceBuilder,
};

/// A generic layer that has been stripped off types.
/// This is returned by a [crate::Backend] and can be used to customize the middleware of the service consuming tasks
pub struct CommonLayer<In, T, U, E> {
    boxed: Arc<dyn Layer<In, Service = BoxCloneService<T, U, E>>>,
}

impl<In, T, U, E> CommonLayer<In, T, U, E> {
    /// Create a new [`CommonLayer`].
    pub fn new<L>(inner_layer: L) -> Self
    where
        L: Layer<In> + 'static,
        L::Service: Service<T, Response = U, Error = E> + Send + 'static + Clone,
        <L::Service as Service<T>>::Future: Send + 'static,
        E: std::error::Error,
    {
        let layer = layer_fn(move |inner: In| {
            let out = inner_layer.layer(inner);
            BoxCloneService::new(out)
        });

        Self {
            boxed: Arc::new(layer),
        }
    }
}

impl<In, T, U, E> Layer<In> for CommonLayer<In, T, U, E> {
    type Service = BoxCloneService<T, U, E>;

    fn layer(&self, inner: In) -> Self::Service {
        self.boxed.layer(inner)
    }
}

impl<In, T, U, E> Clone for CommonLayer<In, T, U, E> {
    fn clone(&self) -> Self {
        Self {
            boxed: Arc::clone(&self.boxed),
        }
    }
}

impl<In, T, U, E> fmt::Debug for CommonLayer<In, T, U, E> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("CommonLayer").finish()
    }
}

/// Extension data for tasks.
pub mod extensions {
    use std::{
        ops::Deref,
        task::{Context, Poll},
    };
    use tower::Service;

    use crate::request::Request;

    /// Extension data for tasks.
    /// This is commonly used to share state across tasks. or across layers within the same tasks
    ///
    /// ```rust
    /// # use std::sync::Arc;
    /// # struct Email;
    /// # use apalis_core::layers::extensions::Data;
    /// # use apalis_core::service_fn::service_fn;
    /// # use crate::apalis_core::builder::WorkerFactory;
    /// # use apalis_core::builder::WorkerBuilder;
    /// # use apalis_core::memory::MemoryStorage;
    /// // Some shared state used throughout our application
    /// struct State {
    ///     // ...
    /// }
    ///
    /// async fn email_service(email: Email, state: Data<Arc<State>>) {
    ///     
    /// }
    ///
    /// let state = Arc::new(State { /* ... */ });
    ///
    /// let worker = WorkerBuilder::new("tasty-avocado")
    ///     .data(state)
    ///     .backend(MemoryStorage::new())
    ///     .build(service_fn(email_service));
    /// ```

    #[derive(Debug, Clone, Copy)]
    pub struct Data<T>(T);
    impl<T> Data<T> {
        /// Build a new data entry
        pub fn new(inner: T) -> Data<T> {
            Data(inner)
        }
    }

    impl<T> Deref for Data<T> {
        type Target = T;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl<S, T> tower::Layer<S> for Data<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        type Service = AddExtension<S, T>;

        fn layer(&self, inner: S) -> Self::Service {
            AddExtension {
                inner,
                value: self.0.clone(),
            }
        }
    }

    /// Middleware for adding some shareable value to [request data].
    #[derive(Clone, Copy, Debug)]
    pub struct AddExtension<S, T> {
        inner: S,
        value: T,
    }

    impl<S, T, Req, Ctx> Service<Request<Req, Ctx>> for AddExtension<S, T>
    where
        S: Service<Request<Req, Ctx>>,
        T: Clone + Send + Sync + 'static,
    {
        type Response = S::Response;
        type Error = S::Error;
        type Future = S::Future;

        #[inline]
        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.inner.poll_ready(cx)
        }

        fn call(&mut self, mut req: Request<Req, Ctx>) -> Self::Future {
            req.parts.data.insert(self.value.clone());
            self.inner.call(req)
        }
    }
}

/// A trait for acknowledging successful processing
/// This trait is called even when a task fails.
/// This is a way of a [`Backend`] to save the result of a job or message
pub trait Ack<Task, Res, Codec> {
    /// The data to fetch from context to allow acknowledgement
    type Context;
    /// The error returned by the ack
    type AckError: std::error::Error;

    /// Acknowledges successful processing of the given request
    fn ack(
        &mut self,
        ctx: &Self::Context,
        response: &Response<Res>,
    ) -> impl Future<Output = Result<(), Self::AckError>> + Send;
}

impl<T, Res: Clone + Send + Sync + Serialize, Ctx: Clone + Send + Sync, Cdc: Codec> Ack<T, Res, Cdc>
    for Sender<(Ctx, Response<Cdc::Compact>)>
where
    Cdc::Error: Debug,
    Cdc::Compact: Send,
{
    type AckError = SendError;
    type Context = Ctx;
    async fn ack(
        &mut self,
        ctx: &Self::Context,
        result: &Response<Res>,
    ) -> Result<(), Self::AckError> {
        let ctx = ctx.clone();
        let res = result.map(|res| Cdc::encode(res).unwrap());
        self.send((ctx, res)).await.unwrap();
        Ok(())
    }
}

/// A layer that acknowledges a job completed successfully
#[derive(Debug)]
pub struct AckLayer<A, Req, Ctx, Cdc> {
    ack: A,
    job_type: PhantomData<Request<Req, Ctx>>,
    codec: PhantomData<Cdc>,
}

impl<A, Req, Ctx, Cdc> AckLayer<A, Req, Ctx, Cdc> {
    /// Build a new [AckLayer] for a job
    pub fn new(ack: A) -> Self {
        Self {
            ack,
            job_type: PhantomData,
            codec: PhantomData,
        }
    }
}

impl<A, Req, Ctx, S, Cdc> Layer<S> for AckLayer<A, Req, Ctx, Cdc>
where
    S: Service<Request<Req, Ctx>> + Send + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
    S::Future: Send + 'static,
    A: Ack<Req, S::Response, Cdc> + Clone + Send + Sync + 'static,
{
    type Service = AckService<S, A, Req, Ctx, Cdc>;

    fn layer(&self, service: S) -> Self::Service {
        AckService {
            service,
            ack: self.ack.clone(),
            job_type: PhantomData,
            codec: PhantomData,
        }
    }
}

/// The underlying service for an [AckLayer]
#[derive(Debug)]
pub struct AckService<SV, A, Req, Ctx, Cdc> {
    service: SV,
    ack: A,
    job_type: PhantomData<Request<Req, Ctx>>,
    codec: PhantomData<Cdc>,
}

impl<Sv: Clone, A: Clone, Req, Ctx, Cdc> Clone for AckService<Sv, A, Req, Ctx, Cdc> {
    fn clone(&self) -> Self {
        Self {
            ack: self.ack.clone(),
            job_type: PhantomData,
            service: self.service.clone(),
            codec: PhantomData,
        }
    }
}

impl<SV, A, Req, Ctx, Cdc> Service<Request<Req, Ctx>> for AckService<SV, A, Req, Ctx, Cdc>
where
    SV: Service<Request<Req, Ctx>> + Send + 'static,
    SV::Error: Into<BoxDynError> + Send + 'static,
    SV::Future: Send + 'static,
    A: Ack<Req, SV::Response, Cdc, Context = Ctx> + Send + 'static + Clone,
    Req: 'static + Send,
    SV::Response: std::marker::Send + Serialize,
    <A as Ack<Req, SV::Response, Cdc>>::Context: Send + Clone,
    <A as Ack<Req, SV::Response, Cdc>>::Context: 'static,
    Ctx: Clone,
{
    type Response = SV::Response;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service
            .poll_ready(cx)
            .map_err(|e| Error::Failed(Arc::new(e.into())))
    }

    fn call(&mut self, request: Request<Req, Ctx>) -> Self::Future {
        let mut ack = self.ack.clone();
        let ctx = request.parts.context.clone();
        let attempt = request.parts.attempt.clone();
        let task_id = request.parts.task_id.clone();
        let fut = self.service.call(request);
        let fut_with_ack = async move {
            let res = fut.await.map_err(|err| {
                let e: BoxDynError = err.into();
                // Try to downcast the error to see if it is already of type `Error`
                if let Some(custom_error) = e.downcast_ref::<Error>() {
                    return custom_error.clone();
                }
                Error::Failed(Arc::new(e))
            });
            let response = Response {
                attempt,
                inner: res,
                task_id,
                _priv: (),
            };
            if let Err(_e) = ack.ack(&ctx, &response).await {
                // TODO: Implement tracing in apalis core
                // tracing::error!("Acknowledgement Failed: {}", e);
            }
            response.inner
        };
        fut_with_ack.boxed()
    }
}
