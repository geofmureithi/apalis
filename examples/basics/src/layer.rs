use std::task::{Context, Poll};

use tower::{Layer, Service};
use tracing::info;

/// A layer that logs a job info before it starts
pub struct LogLayer {
    target: &'static str,
}

impl LogLayer {
    pub fn new(target: &'static str) -> Self {
        Self { target }
    }
}

impl<S> Layer<S> for LogLayer {
    type Service = LogService<S>;

    fn layer(&self, service: S) -> Self::Service {
        LogService {
            target: self.target,
            service,
        }
    }
}

// Example layer service
// This service implements the Log behavior
pub struct LogService<S> {
    target: &'static str,
    service: S,
}

impl<S, Request> Service<Request> for LogService<S>
where
    S: Service<Request>,
    Request: std::fmt::Debug,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        // Use service to apply middleware before or(and) after a request
        info!("request = {:?}, target = {:?}", request, self.target);
        self.service.call(request)
    }
}
