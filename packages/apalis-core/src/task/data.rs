use std::{ops::Deref, task::{Context, Poll}};

use tower_service::Service;

use crate::{task::Task, service_fn::from_request::FromRequest};

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

impl<S, T> tower_layer::Layer<S> for Data<T>
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

impl<S, T, Req, Ctx> Service<Task<Req, Ctx>> for AddExtension<S, T>
where
    S: Service<Task<Req, Ctx>>,
    T: Clone + Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Task<Req, Ctx>) -> Self::Future {
        req.meta.data.insert(self.value.clone());
        self.inner.call(req)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MissingDataError {
    #[error("the type for key `{0}` is not available")]
    NotFound(String),
}

impl<T: Clone + Send + Sync + 'static, Req: Sync, Ctx: Sync> FromRequest<Task<Req, Ctx>>
    for Data<T>
{
    type Error = MissingDataError;
    async fn from_request(req: &Task<Req, Ctx>) -> Result<Self, Self::Error> {
        req.meta.data.get_checked().cloned().map(Data::new)
    }
}
