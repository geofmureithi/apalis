use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use futures::Future;
use tower::{Layer, Service};

use crate::{error::JobError, job::Job, request::JobRequest, response::JobResult};

pub struct PrometheusLayer;

impl<S> Layer<S> for PrometheusLayer {
    type Service = PrometheusService<S>;

    fn layer(&self, service: S) -> Self::Service {
        PrometheusService { service }
    }
}

// This service implements the Log behavior
pub struct PrometheusService<S> {
    service: S,
}

impl<S, J, F> Service<JobRequest<J>> for PrometheusService<S>
where
    S: Service<JobRequest<J>, Response = JobResult, Error = JobError, Future = F>,
    F: Future<Output = Result<JobResult, JobError>> + 'static,
    J: Job,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<JobResult, JobError>>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: JobRequest<J>) -> Self::Future {
        let start = Instant::now();

        let job_type = std::any::type_name::<J>().to_string();
        let req = self.service.call(request);
        let fut = async move {
            let op = J::NAME;
            let response = req.await;

            let latency = start.elapsed().as_secs_f64();
            let status = response
                .as_ref()
                .ok()
                .map(|res| format!("{}", res))
                .unwrap_or("Error".to_string());

            let labels = [
                ("name", op.to_string()),
                ("job_type", job_type),
                ("status", status),
            ];

            metrics::increment_counter!("job_requests_total", &labels);
            metrics::histogram!("job_requests_duration_seconds", latency, &labels);
            response
        };
        Box::pin(fut)
    }
}
