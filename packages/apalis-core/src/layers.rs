use crate::task::attempt::Attempt;
use crate::{request::Request, worker::WorkerId};
use futures::channel::mpsc::{SendError, Sender};
use futures::SinkExt;
use futures::{future::BoxFuture, Future, FutureExt};
use serde::{Deserialize, Serialize};
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

    impl<S, T, Req> Service<Request<Req>> for AddExtension<S, T>
    where
        S: Service<Request<Req>>,
        T: Clone + Send + Sync + 'static,
    {
        type Response = S::Response;
        type Error = S::Error;
        type Future = S::Future;

        #[inline]
        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.inner.poll_ready(cx)
        }

        fn call(&mut self, mut req: Request<Req>) -> Self::Future {
            req.data.insert(self.value.clone());
            self.inner.call(req)
        }
    }
}

/// A trait for acknowledging successful processing
/// This trait is called even when a task fails.
/// This is a way of a [`Backend`] to save the result of a job or message
pub trait Ack<Task> {
    /// The data to fetch from context to allow acknowledgement
    type Acknowledger;
    /// The error returned by the ack
    type Error: std::error::Error;
    /// Acknowledges successful processing of the given request
    fn ack(
        &mut self,
        response: AckResponse<Self::Acknowledger>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// ACK response
#[derive(Debug, Serialize, Deserialize)]
pub struct AckResponse<A> {
    /// The worker id
    pub worker: WorkerId,
    /// The acknowledger
    pub acknowledger: A,
    /// The stringified result
    pub result: Result<String, String>,
    /// The number of attempts made by the request
    pub attempts: Attempt,
}

/// A generic stream that emits (worker_id, task_id)
#[derive(Debug, Clone)]
pub struct AckStream<A>(pub Sender<AckResponse<A>>);

impl<J, A: Send + Clone + 'static> Ack<J> for AckStream<A> {
    type Acknowledger = A;
    type Error = SendError;
    fn ack(
        &mut self,
        response: AckResponse<A>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.0.send(response).boxed()
    }
}

/// A layer that acknowledges a job completed successfully
#[derive(Debug)]
pub struct AckLayer<A: Ack<J>, J> {
    ack: A,
    job_type: PhantomData<J>,
    worker_id: WorkerId,
}

impl<A: Ack<J>, J> AckLayer<A, J> {
    /// Build a new [AckLayer] for a job
    pub fn new(ack: A, worker_id: WorkerId) -> Self {
        Self {
            ack,
            job_type: PhantomData,
            worker_id,
        }
    }
}

impl<A, J, S> Layer<S> for AckLayer<A, J>
where
    S: Service<Request<J>> + Send + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
    S::Future: Send + 'static,
    A: Ack<J> + Clone + Send + Sync + 'static,
{
    type Service = AckService<S, A, J>;

    fn layer(&self, service: S) -> Self::Service {
        AckService {
            service,
            ack: self.ack.clone(),
            job_type: PhantomData,
            worker_id: self.worker_id.clone(),
        }
    }
}

/// The underlying service for an [AckLayer]
#[derive(Debug)]
pub struct AckService<SV, A, J> {
    service: SV,
    ack: A,
    job_type: PhantomData<J>,
    worker_id: WorkerId,
}

impl<Sv: Clone, A: Clone, J> Clone for AckService<Sv, A, J> {
    fn clone(&self) -> Self {
        Self {
            ack: self.ack.clone(),
            job_type: PhantomData,
            worker_id: self.worker_id.clone(),
            service: self.service.clone(),
        }
    }
}

impl<SV, A, T> Service<Request<T>> for AckService<SV, A, T>
where
    SV: Service<Request<T>> + Send + Sync + 'static,
    SV::Error: std::error::Error + Send + Sync + 'static,
    <SV as Service<Request<T>>>::Future: std::marker::Send + 'static,
    A: Ack<T> + Send + 'static + Clone + Send + Sync,
    T: 'static,
    <SV as Service<Request<T>>>::Response: std::marker::Send + fmt::Debug + Sync,
    <A as Ack<T>>::Acknowledger: Sync + Send + Clone,
{
    type Response = SV::Response;
    type Error = SV::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request<T>) -> Self::Future {
        let mut ack = self.ack.clone();
        let worker_id = self.worker_id.clone();
        let data = request.get::<<A as Ack<T>>::Acknowledger>().cloned();
        let attempts = request.get::<Attempt>().cloned().unwrap_or_default();

        let fut = self.service.call(request);
        let fut_with_ack = async move {
            let res = fut.await;
            let result = res
                .as_ref()
                .map(|ok| format!("{ok:?}"))
                .map_err(|e| e.to_string());
            if let Some(task_id) = data {
                if let Err(_e) = ack
                    .ack(AckResponse {
                        worker: worker_id,
                        acknowledger: task_id,
                        result,
                        attempts,
                    })
                    .await
                {
                    // TODO: Implement tracing in apalis core
                    // tracing::error!("Acknowledgement Failed: {}", e);
                }
            } else {
                // tracing::error!(
                //     "Acknowledgement could not be called due to missing ack data in context : {}",
                //     &std::any::type_name::<<A as Ack<T>>::Acknowledger>()
                // );
            }
            res
        };
        fut_with_ack.boxed()
    }
}
