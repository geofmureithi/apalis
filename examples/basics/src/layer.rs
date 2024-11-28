use std::{
    fmt::Debug,
    task::{Context, Poll},
};

use apalis::prelude::Request;
use tower::{Layer, Service};
use tracing::info;

/// A layer that logs a job info before it starts
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
pub struct LogService<S> {
    target: &'static str,
    service: S,
}

impl<S, Req, Ctx> Service<Request<Req, Ctx>> for LogService<S>
where
    S: Service<Request<Req, Ctx>> + Clone,
    Req: Debug,
    Ctx: Debug,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Req, Ctx>) -> Self::Future {
        // Use service to apply middleware before or(and) after a request
        info!("request = {:?}, target = {:?}", request, self.target);
        self.service.call(request)
    }
}
