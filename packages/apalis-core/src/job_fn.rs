use crate::context::JobContext;
use crate::job::Job;
use crate::request::JobRequest;
use crate::response::IntoResponse;

use futures::future::Map;
use futures::FutureExt;

use std::fmt;
use std::future::Future;
use std::task::{Context, Poll};
use tower::Service;

/// A helper method to build job functions
pub fn job_fn<T>(f: T) -> JobFn<T> {
    JobFn { f }
}

/// A [`Job`] implemented by a closure.
///
/// See [`job_fn`] for more details.
#[derive(Copy, Clone)]
pub struct JobFn<T> {
    f: T,
}

impl<T> fmt::Debug for JobFn<T>
where
    T: Job,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobFn")
            .field("f", &format_args!("{}", T::NAME))
            .finish()
    }
}

/// The Future returned from [`JobFn`] service.
pub type JobFnFuture<F, O, R, E> = Map<F, fn(O) -> std::result::Result<R, E>>;

impl<T, F, Request, E, R> Service<JobRequest<Request>> for JobFn<T>
where
    T: Fn(Request, JobContext) -> F,
    F: Future,
    F::Output: IntoResponse<Result = std::result::Result<R, E>>,
{
    type Response = R;
    type Error = E;
    type Future = JobFnFuture<F, F::Output, R, E>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, job: JobRequest<Request>) -> Self::Future {
        let fut = (self.f)(job.job, job.context);

        fut.map(F::Output::into_response)
    }
}
