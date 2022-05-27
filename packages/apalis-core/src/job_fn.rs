use std::fmt::{self, Debug};
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
/// ```
/// use tower::{job_fn, Job, JobExt, BoxError};
/// # struct Request;
/// # impl Request {
/// #     fn new() -> Self { Self }
/// # }
/// # struct Response(&'static str);
/// # impl Response {
/// #     fn new(body: &'static str) -> Self {
/// #         Self(body)
/// #     }
/// #     fn into_body(self) -> &'static str { self.0 }
/// # }
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), BoxError> {
/// async fn handle(request: Request) -> Result<Response, BoxError> {
///     let response = Response::new("Hello, World!");
///     Ok(response)
/// }
///
/// let mut job = job_fn(handle);
///
/// let response = job
///     .ready()
///     .await?
///     .call(Request::new())
///     .await?;
///
/// assert_eq!("Hello, World!", response.into_body());
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

impl<T, F, Request, IR> Service<JobRequest<Request>> for JobFn<T>
where
    Request: Debug + 'static,
    T: Fn(Request, JobContext) -> F,
    F: Future<Output = IR> + 'static,
    Request: Job,
    IR: IntoJobResponse,
{
    type Response = JobResult;
    type Error = JobError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + 'static>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, job: JobRequest<Request>) -> Self::Future {
        let fut = (self.f)(job.job, job.context);
        let fut = async move {
            let res = fut.await;
            res.into_response()
        };
        Box::pin(fut)
    }
}
