use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use futures::Future;
use pin_project_lite::pin_project;
use tower::{Layer, Service};

use crate::{error::JobError, job::Job, request::JobRequest};

/// A layer to support prometheus metrics
#[derive(Debug)]
pub struct PrometheusLayer;

impl<S> Layer<S> for PrometheusLayer {
    type Service = PrometheusService<S>;

    fn layer(&self, service: S) -> Self::Service {
        PrometheusService { service }
    }
}

/// This service implements the metric collection behavior
#[derive(Debug)]
pub struct PrometheusService<S> {
    service: S,
}

impl<S, J, F, Res> Service<JobRequest<J>> for PrometheusService<S>
where
    S: Service<JobRequest<J>, Response = Res, Error = JobError, Future = F>,
    F: Future<Output = Result<Res, JobError>> + 'static,
    J: Job,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<F>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: JobRequest<J>) -> Self::Future {
        let start = Instant::now();
        let req = self.service.call(request);
        let job_type = std::any::type_name::<J>().to_string();
        let op = J::NAME;
        ResponseFuture {
            inner: req,
            start,
            job_type,
            operation: op.to_string(),
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
    Fut: Future<Output = Result<Res, JobError>>,
{
    type Output = Result<Res, JobError>;

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
            ("job_type", this.job_type.to_string()),
            ("status", status),
        ];

        metrics::increment_counter!("job_requests_total", &labels);
        metrics::histogram!("job_requests_duration_seconds", latency, &labels);
        Poll::Ready(response)
    }
}
