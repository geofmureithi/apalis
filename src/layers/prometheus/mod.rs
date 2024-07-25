use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use apalis_core::{error::Error, request::Request, task::namespace::Namespace};
use futures::Future;
use pin_project_lite::pin_project;
use tower::{Layer, Service};

/// A layer to support prometheus metrics
#[derive(Debug, Default)]
pub struct PrometheusLayer;

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

impl<S, J, F, Res> Service<Request<J>> for PrometheusService<S>
where
    S: Service<Request<J>, Response = Res, Error = Error, Future = F>,
    F: Future<Output = Result<Res, Error>> + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<F>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request<J>) -> Self::Future {
        let start = Instant::now();
        let namespace = request.get::<Namespace>().unwrap().to_string();

        let req = self.service.call(request);
        let job_type = std::any::type_name::<J>().to_string();

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
