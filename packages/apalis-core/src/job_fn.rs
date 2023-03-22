use crate::context::JobContext;
use crate::job::Job;
use crate::request::JobRequest;
use crate::response::IntoResponse;

use futures::future::BoxFuture;

use std::fmt::{self};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::{BoxError, Service};

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

impl<T, F, Res: 'static, Request, E: 'static, R: 'static> Service<JobRequest<Request>> for JobFn<T>
where
    Request: 'static,
    T: Fn(Request, JobContext) -> F,
    Res: IntoResponse<Result = std::result::Result<R, E>> + 'static,
    F: Future<Output = Res> + 'static + Send,
    Request: Job,
    E: Into<BoxError> + Send + Sync,
{
    type Response = R;
    type Error = E;
    // TODO: Improve this to remove the send
    type Future = JobFnHttpFuture<BoxFuture<'static, std::result::Result<R, E>>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, job: JobRequest<Request>) -> Self::Future {
        let fut = (self.f)(job.job, job.context);

        JobFnHttpFuture {
            future: Box::pin(async { fut.await.into_response() }),
        }
    }
}
