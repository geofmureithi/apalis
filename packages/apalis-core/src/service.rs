use futures::Future;
use std::{
    fmt::Debug,
    pin::Pin,
    task::{Context, Poll},
};
use tower::Service;
use tracing::Span;
use tracing_futures::Instrument;

use crate::{
    error::JobError,
    job::{Job, JobHandler},
    request::JobRequest,
    response::JobResult,
};

/// Represents the default [JobService].
/// Used to spawn all jobs that implement [Job]
#[derive(Clone)]
pub struct JobService;

impl<Request> Service<JobRequest<Request>> for JobService
where
    Request: Debug + Job + 'static + JobHandler<Request>,
{
    type Response = JobResult;
    type Error = JobError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, job: JobRequest<Request>) -> Self::Future {
        let span = Span::current();
        let fut = async move {
            let res = job.do_handle().await;
            res
        }
        .instrument(span);
        Box::pin(fut)
    }
}
