use futures::future::BoxFuture;
use futures::FutureExt;
use std::fmt::{self};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::Service;

use crate::context::JobContext;
use crate::error::JobError;
use crate::job::Job;
use crate::request::JobRequest;
use crate::response::{IntoJobResponse, JobResult};

/// Returns a new [`JobFn`] with the given closure.
///
/// This lets you build a [`Job`] from an async function that returns a [`Result`].
///
/// # Example
///
/// ```rust,ignore
/// use apalis_core::{job_fn, Job};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), BoxError> {
/// async fn handle(request: JobRequest) -> Result<JobResult, BoxError> {
///     Ok(JobResult::Ok)
/// }
///
/// let mut job = job_fn(handle);
///
/// let response = job
///     .ready()
///     .await?
///     .call(JobRequest::new())
///     .await?;
///
/// assert_eq!(JobResult::Ok, response);
/// #
/// # Ok(())
/// # }
/// ```
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

pin_project_lite::pin_project! {
    /// The Future returned from [`JobFn`] service.
    pub struct JobFnHttpFuture<F> {
        #[pin]
        future: F,
    }
}

impl<F, Res> Future for JobFnHttpFuture<F>
where
    F: Future<Output = Res> + 'static,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let slf = self.project();
        slf.future.poll(cx)
    }
}

impl<T, F, Res, Request> Service<JobRequest<Request>> for JobFn<T>
where
    Request: 'static,
    T: Fn(Request, JobContext) -> F,
    Res: IntoJobResponse,
    F: Future<Output = Res> + 'static + Send,
    Request: Job,
{
    type Response = JobResult;
    type Error = JobError;
    // TODO: Improve this to remove the send
    type Future = JobFnHttpFuture<BoxFuture<'static, Result<JobResult, JobError>>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, job: JobRequest<Request>) -> Self::Future {
        let fut = (self.f)(job.job, job.context).map(|res| res.into_response());

        JobFnHttpFuture {
            future: Box::pin(fut),
        }
    }
}
