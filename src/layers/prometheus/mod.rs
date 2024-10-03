use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use apalis_core::{error::Error, request::Request};
use futures::Future;
use pin_project_lite::pin_project;
use tower::{Layer, Service};

/// A layer to support prometheus metrics
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct PrometheusLayer {}

impl<S> Layer<S> for PrometheusLayer {
    type Service = PrometheusService<S>;

    fn layer(&self, service: S) -> Self::Service {
        PrometheusService { service }
    }
}

/// This service implements the metric collection behavior
#[derive(Debug, Clone)]
pub struct PrometheusService<S> {
    service: S,
}

impl<Svc, Fut, Req, Ctx, Res> Service<Request<Req, Ctx>> for PrometheusService<Svc>
where
    Svc: Service<Request<Req, Ctx>, Response = Res, Error = Error, Future = Fut>,
    Fut: Future<Output = Result<Res, Error>> + 'static,
{
    type Response = Svc::Response;
    type Error = Svc::Error;
    type Future = ResponseFuture<Fut>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Req, Ctx>) -> Self::Future {
        let start = Instant::now();
        let namespace = request
            .parts
            .namespace
            .as_ref()
            .map(|ns| ns.0.to_string())
            .unwrap_or(std::any::type_name::<Svc>().to_string());

        let req = self.service.call(request);
        let job_type = std::any::type_name::<Req>().to_string();

        ResponseFuture {
            inner: req,
            start,
            job_type,
            operation: namespace,
        }
    }
}

pin_project! {
    /// Response for prometheus service
    pub struct ResponseFuture<F> {
        #[pin]
        pub(crate) inner: F,
        pub(crate) start: Instant,
        pub(crate) job_type: String,
        pub(crate) operation: String
    }
}

impl<Fut, Res> Future for ResponseFuture<Fut>
where
    Fut: Future<Output = Result<Res, Error>>,
{
    type Output = Result<Res, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let response = futures::ready!(this.inner.poll(cx));

        let latency = this.start.elapsed().as_secs_f64();
        let status = response
            .as_ref()
            .ok()
            .map(|_res| "Ok".to_string())
            .unwrap_or_else(|| "Err".to_string());

        let labels = [
            ("name", this.operation.to_string()),
            ("namespace", this.job_type.to_string()),
            ("status", status),
        ];
        let counter = metrics::counter!("requests_total", &labels);
        counter.increment(1);
        let hist = metrics::histogram!("request_duration_seconds", &labels);
        hist.record(latency);
        Poll::Ready(response)
    }
}
