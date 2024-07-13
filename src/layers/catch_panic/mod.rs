use std::fmt;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::task::{Context, Poll};

use apalis_core::error::Error;
use apalis_core::request::Request;
use backtrace::Backtrace;
use tower::Layer;
use tower::Service;

/// Apalis Layer that catches panics in the service.
#[derive(Clone, Debug)]
pub struct CatchPanicLayer;

impl CatchPanicLayer {
    /// Creates a new `CatchPanicLayer`.
    pub fn new() -> Self {
        CatchPanicLayer
    }
}

impl Default for CatchPanicLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Layer<S> for CatchPanicLayer {
    type Service = CatchPanicService<S>;

    fn layer(&self, service: S) -> Self::Service {
        CatchPanicService { service }
    }
}

/// Apalis Service that catches panics.
#[derive(Clone, Debug)]
pub struct CatchPanicService<S> {
    service: S,
}

impl<S, J, Res> Service<Request<J>> for CatchPanicService<S>
where
    S: Service<Request<J>, Response = Res, Error = Error>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = CatchPanicFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request<J>) -> Self::Future {
        CatchPanicFuture {
            future: self.service.call(request),
        }
    }
}

pin_project_lite::pin_project! {
    /// A wrapper that catches panics during execution
    pub struct CatchPanicFuture<F> {
        #[pin]
        future: F,

    }
}

/// An error generated from a panic
#[derive(Debug, Clone)]
pub struct PanicError(pub String, pub Backtrace);

impl std::error::Error for PanicError {}

impl fmt::Display for PanicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PanicError: {}, Backtrace: {:?}", self.0, self.1)
    }
}

impl<F, Res> Future for CatchPanicFuture<F>
where
    F: Future<Output = Result<Res, Error>>,
{
    type Output = Result<Res, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match catch_unwind(AssertUnwindSafe(|| this.future.poll(cx))) {
            Ok(res) => res,
            Err(e) => {
                let panic_info = if let Some(s) = e.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Unknown panic".to_string()
                };
                Poll::Ready(Err(Error::Failed(Box::new(PanicError(
                    panic_info,
                    Backtrace::new(),
                )))))
            }
        }
    }
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

    impl Service<Request<TestJob>> for TestService {
        type Response = usize;
        type Error = Error;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<TestJob>) -> Self::Future {
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

        impl Service<Request<TestJob>> for PanicService {
            type Response = usize;
            type Error = Error;
            type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

            fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, _req: Request<TestJob>) -> Self::Future {
                Box::pin(async { None.unwrap() })
            }
        }

        let layer = CatchPanicLayer::new();
        let mut service = layer.layer(PanicService);

        let request = Request::new(TestJob);
        let response = service.call(request).await;

        assert!(response.is_err());

        assert_eq!(
            response.unwrap_err().to_string()[0..87],
            *"FailedError: PanicError: called `Option::unwrap()` on a `None` value, Backtrace:    0: "
        );
    }
}
