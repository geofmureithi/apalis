use std::any::Any;
use std::fmt;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use apalis_core::error::Error;
use apalis_core::request::Request;
use tower::Layer;
use tower::Service;

/// Apalis Layer that catches panics in the service.
#[derive(Clone, Debug)]
pub struct CatchPanicLayer<F> {
    on_panic: F,
}

impl CatchPanicLayer<fn(Box<dyn Any + Send>) -> Error> {
    /// Creates a new `CatchPanicLayer` with a default panic handler.
    pub fn new() -> Self {
        CatchPanicLayer {
            on_panic: default_handler,
        }
    }
}

impl Default for CatchPanicLayer<fn(Box<dyn Any + Send>) -> Error> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F> CatchPanicLayer<F>
where
    F: FnMut(Box<dyn Any + Send>) -> Error + Clone,
{
    /// Creates a new `CatchPanicLayer` with a custom panic handler.
    pub fn with_panic_handler(on_panic: F) -> Self {
        CatchPanicLayer { on_panic }
    }
}

impl<S, F> Layer<S> for CatchPanicLayer<F>
where
    F: FnMut(Box<dyn Any + Send>) -> Error + Clone,
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

impl<S, Req, Res, Ctx, F> Service<Request<Req, Ctx>> for CatchPanicService<S, F>
where
    S: Service<Request<Req, Ctx>, Response = Res, Error = Error>,
    F: FnMut(Box<dyn Any + Send>) -> Error + Clone,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = CatchPanicFuture<S::Future, F>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Req, Ctx>) -> Self::Future {
        CatchPanicFuture {
            future: self.service.call(request),
            on_panic: self.on_panic.clone(),
        }
    }
}

pin_project_lite::pin_project! {
    /// A wrapper that catches panics during execution
    pub struct CatchPanicFuture<Fut, F> {
        #[pin]
        future: Fut,
        on_panic: F,
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

impl<Fut, Res, F> Future for CatchPanicFuture<Fut, F>
where
    Fut: Future<Output = Result<Res, Error>>,
    F: FnMut(Box<dyn Any + Send>) -> Error,
{
    type Output = Result<Res, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().project();

        match catch_unwind(AssertUnwindSafe(|| this.future.poll(cx))) {
            Ok(res) => res,
            Err(e) => Poll::Ready(Err((this.on_panic)(e))),
        }
    }
}

fn default_handler(e: Box<dyn Any + Send>) -> Error {
    let panic_info = if let Some(s) = e.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = e.downcast_ref::<String>() {
        s.clone()
    } else {
        "Unknown panic".to_string()
    };
    // apalis assumes service functions are pure
    // therefore a panic should ideally abort
    Error::Abort(Arc::new(Box::new(PanicError(panic_info))))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::task::{Context, Poll};
    use tower::Service;

    #[derive(Clone, Debug)]
    struct TestJob;

    #[derive(Clone)]
    struct TestService;

    impl Service<Request<TestJob, ()>> for TestService {
        type Response = usize;
        type Error = Error;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<TestJob, ()>) -> Self::Future {
            Box::pin(async { Ok(42) })
        }
    }

    #[tokio::test]
    async fn test_catch_panic_layer() {
        let layer = CatchPanicLayer::new();
        let mut service = layer.layer(TestService);

        let request = Request::new(TestJob);
        let response = service.call(request).await;

        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_catch_panic_layer_panics() {
        struct PanicService;

        impl Service<Request<TestJob, ()>> for PanicService {
            type Response = usize;
            type Error = Error;
            type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

            fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, _req: Request<TestJob, ()>) -> Self::Future {
                Box::pin(async { None.unwrap() })
            }
        }

        let layer = CatchPanicLayer::new();
        let mut service = layer.layer(PanicService);

        let request = Request::new(TestJob);
        let response = service.call(request).await;

        assert!(response.is_err());

        assert_eq!(
            response.unwrap_err().to_string(),
            *"AbortError: PanicError: called `Option::unwrap()` on a `None` value"
        );
    }
}
