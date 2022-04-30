use std::{
    fmt::Debug,
    pin::Pin,
    task::{Context, Poll},
};

use actix::clock::Instant;
use futures::Future;
use tower::Service;

use crate::{error::JobError, job::Job, request::JobRequest, response::JobResult};

pub struct JobService;

impl<Request> Service<JobRequest<Request>> for JobService
where
    Request: Debug + Job + 'static,
{
    type Response = JobResult;
    type Error = JobError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut job: JobRequest<Request>) -> Self::Future {
        let id = job.id();
        let fut = async move {
            let now = Instant::now();
            log::debug!(
                target: Request::NAME,
                "JobService: [{}] ready for processing.",
                id
            );
            let res = job.do_handle().await;
            log::debug!(
                "JobService: [{}] completed in {} μs",
                id,
                now.elapsed().as_micros()
            );

            res
        };
        Box::pin(fut)
    }
}
