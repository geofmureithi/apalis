use std::task::{Context, Poll};
use tower::Service;

use crate::request::JobRequest;

/// Extension data for jobs.
///
/// forked from [axum::Extensions]
/// # In Context
///
/// This is commonly used to share state across jobs.
///
/// ```rust,no_run
/// use apalis::{
///     Extension,
///     WorkerBuilder,
///     JobContext
/// };
/// use std::sync::Arc;
///
/// // Some shared state used throughout our application
/// struct State {
///     // ...
/// }
///
/// async fn email_service(email: Email, ctx: JobContext) {
///     let state: &Arc<State> = ctx.data_opt().unwrap()
/// }
///
/// let state = Arc::new(State { /* ... */ });
///
/// let worker = WorkerBuilder::new(storage)
///     .layer(Extension(state))
///     .build_fn(email_service);
/// ```

#[derive(Debug, Clone, Copy)]
pub struct Extension<T>(pub T);

impl<S, T> ::tower::Layer<S> for Extension<T>
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

/// Middleware for adding some shareable value to [request extensions].
///
/// See [Sharing state with handlers](index.html#sharing-state-with-handlers)
/// for more details.
///
/// [request extensions]: https://docs.rs/http/latest/http/struct.Extensions.html
#[derive(Clone, Copy, Debug)]
pub struct AddExtension<S, T> {
    pub(crate) inner: S,
    pub(crate) value: T,
}

impl<J, S, T> Service<JobRequest<J>> for AddExtension<S, T>
where
    S: Service<JobRequest<J>>,
    T: Clone + Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: JobRequest<J>) -> Self::Future {
        req.context_mut().insert(self.value.clone());
        self.inner.call(req)
    }
}
