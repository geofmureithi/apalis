use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

use apalis_core::{task::Task, worker::context::WorkerContext};
use futures::Future;
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

impl<Svc, Fut, Args, Ctx, Res, Err, IdType> Service<Task<Args, Ctx, IdType>>
    for PrometheusService<Svc>
where
    Svc: Service<Task<Args, Ctx, IdType>, Response = Res, Error = Err, Future = Fut>,
    Fut: Future<Output = Result<Res, Err>> + 'static,
{
    type Response = Svc::Response;
    type Error = Svc::Error;
    type Future = ResponseFuture<Fut>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Task<Args, Ctx, IdType>) -> Self::Future {
        let start = Instant::now();
        let worker = request
            .parts
            .data
            .get::<WorkerContext>()
            .map(|w| w.name())
            .cloned()
            .expect("worker context not found in task data");

        let req = self.service.call(request);
        let job_type = std::any::type_name::<Args>().to_string();

        ResponseFuture {
            inner: req,
            start,
            job_type,
            worker,
        }
    }
}

/// Response for prometheus service
#[pin_project::pin_project]
pub struct ResponseFuture<F> {
    #[pin]
    pub(crate) inner: F,
    pub(crate) start: Instant,
    pub(crate) job_type: String,
    pub(crate) worker: String,
}

impl<F> fmt::Debug for ResponseFuture<F>
where
    F: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let elapsed = self.start.elapsed();

        f.debug_struct("ResponseFuture")
            .field("inner", &self.inner)
            .field("elapsed_since_start", &elapsed)
            .field("job_type", &self.job_type)
            .field("worker", &self.worker)
            .finish()
    }
}

impl<Fut, Res, Err> Future for ResponseFuture<Fut>
where
    Fut: Future<Output = Result<Res, Err>>,
{
    type Output = Result<Res, Err>;

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
            ("worker", this.worker.to_string()),
            ("queue", this.job_type.to_string()),
            ("status", status),
        ];
        let counter = metrics::counter!("tasks_total", &labels);
        counter.increment(1);
        let hist = metrics::histogram!("task_duration_seconds", &labels);
        hist.record(latency);
        Poll::Ready(response)
    }
}
