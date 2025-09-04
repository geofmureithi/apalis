use apalis_core::error::AbortError;
use apalis_core::task::Task;
use std::any::Any;
use std::fmt;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::Layer;
use tower::Service;

/// Apalis Layer that catches panics in the service.
#[derive(Clone, Debug)]
pub struct CatchPanicLayer<F, Err> {
    on_panic: F,
    _marker: std::marker::PhantomData<Err>,
}

impl CatchPanicLayer<fn(Box<dyn Any + Send + 'static>) -> AbortError, AbortError> {
    /// Creates a new `CatchPanicLayer` with a default panic handler.
    pub fn new() -> Self {
        CatchPanicLayer {
            on_panic: default_handler,
            _marker: std::marker::PhantomData,
        }
    }
}

impl Default for CatchPanicLayer<fn(Box<dyn Any + Send>) -> AbortError, AbortError> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F, Err> CatchPanicLayer<F, Err>
where
    F: FnMut(Box<dyn Any + Send>) -> Err + Clone,
{
    /// Creates a new `CatchPanicLayer` with a custom panic handler.
    pub fn with_panic_handler(on_panic: F) -> Self {
        CatchPanicLayer {
            on_panic,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S, F, Err> Layer<S> for CatchPanicLayer<F, Err>
where
    F: FnMut(Box<dyn Any + Send>) -> Err + Clone,
{
    type Service = CatchPanicService<S, F>;

    fn layer(&self, service: S) -> Self::Service {
        CatchPanicService {
            service,
            on_panic: self.on_panic.clone(),
        }
    }
}

/// Apalis Service that catches panics.
#[derive(Clone, Debug)]
pub struct CatchPanicService<S, F> {
    service: S,
    on_panic: F,
}

impl<S, Req, Res, Ctx, F, Err> Service<Task<Req, Ctx>> for CatchPanicService<S, F>
where
    S: Service<Task<Req, Ctx>, Response = Res, Error = Err>,
    F: FnMut(Box<dyn Any + Send>) -> Err + Clone,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = CatchPanicFuture<S::Future, F, Err>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Task<Req, Ctx>) -> Self::Future {
        CatchPanicFuture {
            future: self.service.call(request),
            on_panic: self.on_panic.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

pin_project_lite::pin_project! {
    /// A wrapper that catches panics during execution
    pub struct CatchPanicFuture<Fut, F, Err> {
        #[pin]
        future: Fut,
        on_panic: F,
        _marker: std::marker::PhantomData<Err>,
    }
}

/// An error generated from a panic
#[derive(Debug, Clone)]
pub struct PanicError(pub String);

impl std::error::Error for PanicError {}

impl fmt::Display for PanicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PanicError: {}", self.0)
    }
}

impl<Fut, Res, F, Err> Future for CatchPanicFuture<Fut, F, Err>
where
    Fut: Future<Output = Result<Res, Err>>,
    F: FnMut(Box<dyn Any + Send>) -> Err,
{
    type Output = Result<Res, Err>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().project();

        match catch_unwind(AssertUnwindSafe(|| this.future.poll(cx))) {
            Ok(res) => res,
            Err(e) => Poll::Ready(Err((this.on_panic)(e))),
        }
    }
}

fn default_handler(e: Box<dyn Any + Send>) -> AbortError {
    let panic_info = if let Some(s) = e.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = e.downcast_ref::<String>() {
        s.clone()
    } else {
        "Unknown panic".to_string()
    };
    // apalis assumes service functions are pure
    // therefore a panic should ideally abort
    AbortError::new(PanicError(panic_info))
}

#[cfg(test)]
mod tests {
    use super::*;

    use apalis_core::error::BoxDynError;
    use std::task::{Context, Poll};
    use tower::{Service, ServiceExt};

    #[derive(Clone, Debug)]
    struct TestJob;

    #[derive(Clone)]
    struct TestService;

    impl Service<Task<TestJob, ()>> for TestService {
        type Response = usize;
        type Error = AbortError;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Task<TestJob, ()>) -> Self::Future {
            Box::pin(async { Ok(42) })
        }
    }

    #[tokio::test]
    async fn test_catch_panic_layer() {
        let layer = CatchPanicLayer::new();
        let mut service = layer.layer(TestService);

        let request = Task::new(TestJob);
        let response = service.call(request).await;

        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_catch_panic_layer_panics() {
        struct PanicService;

        impl Service<Task<TestJob, ()>> for PanicService {
            type Response = usize;
            type Error = AbortError;
            type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

            fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, _req: Task<TestJob, ()>) -> Self::Future {
                Box::pin(async { None.unwrap() })
            }
        }

        let layer = CatchPanicLayer::new();
        let mut service = layer.layer(PanicService);

        let request = Task::new(TestJob);
        let response = service.call(request).await;

        assert!(response.is_err());

        assert_eq!(
            response.unwrap_err().to_string(),
            *"AbortError: PanicError: called `Option::unwrap()` on a `None` value"
        );
    }
}
