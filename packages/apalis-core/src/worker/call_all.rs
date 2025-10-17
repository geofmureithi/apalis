//! Utilities for executing all tasks from a stream to a service.
//!
//! A combinator for calling all requests from a stream to a service, yielding responses
//! as they arrive. It supports both ordered and unordered response handling, allowing for flexible integration
//! with asynchronous services.
use futures_util::{Stream, ready, stream::FuturesUnordered};
use std::{
    error::Error,
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tower_service::Service;

use crate::error::BoxDynError;

/// A stream of responses received from the inner service in received order.
#[derive(Debug)]
#[pin_project::pin_project]
pub(super) struct CallAllUnordered<Svc, S, T, E>
where
    Svc: Service<T>,
    S: Stream<Item = Result<Option<T>, E>>,
{
    #[pin]
    inner: CallAll<Svc, S, T, FuturesUnordered<Svc::Future>, E>,
}

impl<Svc, S, T, E> CallAllUnordered<Svc, S, T, E>
where
    Svc: Service<T>,
    S: Stream<Item = Result<Option<T>, E>>,
{
    /// Create new [`CallAllUnordered`] combinator.
    pub(super) fn new(service: Svc, stream: S) -> CallAllUnordered<Svc, S, T, E> {
        CallAllUnordered {
            inner: CallAll::new(service, stream, FuturesUnordered::new()),
        }
    }
}

impl<Svc, S, T, E> Stream for CallAllUnordered<Svc, S, T, E>
where
    Svc: Service<T>,
    S: Stream<Item = Result<Option<T>, E>>,
    E: Into<BoxDynError>,
{
    type Item = Result<Option<Svc::Response>, CallAllError<Svc::Error>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

/// Error type that combines stream errors and service errors
#[derive(Debug)]
pub enum CallAllError<ServiceError> {
    /// Error originating from the request stream
    StreamError(BoxDynError),
    /// Error originating from the service
    ServiceError(ServiceError),
}

impl<SE: fmt::Display> fmt::Display for CallAllError<SE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallAllError::StreamError(e) => write!(f, "Stream error: {e}"),
            CallAllError::ServiceError(e) => write!(f, "Service error: {e}"),
        }
    }
}

impl<SE: Error + 'static> Error for CallAllError<SE> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            CallAllError::StreamError(e) => Some(e.as_ref()),
            CallAllError::ServiceError(e) => Some(e),
        }
    }
}

impl<F: Future> Drive<F> for FuturesUnordered<F> {
    fn is_empty(&self) -> bool {
        FuturesUnordered::is_empty(self)
    }

    fn push(&mut self, future: F) {
        FuturesUnordered::push(self, future)
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Option<F::Output>> {
        Stream::poll_next(Pin::new(self), cx)
    }
}

/// The [`Future`] returned by the [`ServiceExt::call_all`] combinator.
#[pin_project::pin_project]
pub(crate) struct CallAll<Svc, S, T, Q, E>
where
    S: Stream<Item = Result<Option<T>, E>>,
{
    service: Option<Svc>,
    #[pin]
    stream: S,
    queue: Q,
    eof: bool,
    curr_req: Option<T>,
}

impl<Svc, S, T, Q, E> fmt::Debug for CallAll<Svc, S, T, Q, E>
where
    Svc: fmt::Debug,
    S: Stream<Item = Result<Option<T>, E>> + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CallAll")
            .field("service", &self.service)
            .field("stream", &self.stream)
            .field("eof", &self.eof)
            .finish()
    }
}

pub(crate) trait Drive<F: Future> {
    fn is_empty(&self) -> bool;

    fn push(&mut self, future: F);

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Option<F::Output>>;
}

impl<Svc, S, T, Q, E> CallAll<Svc, S, T, Q, E>
where
    Svc: Service<T>,
    S: Stream<Item = Result<Option<T>, E>>,
    Q: Drive<Svc::Future>,
{
    pub(crate) const fn new(service: Svc, stream: S, queue: Q) -> CallAll<Svc, S, T, Q, E> {
        CallAll {
            service: Some(service),
            stream,
            queue,
            eof: false,
            curr_req: None,
        }
    }
}

impl<Svc, S, T, Q, E> Stream for CallAll<Svc, S, T, Q, E>
where
    Svc: Service<T>,
    S: Stream<Item = Result<Option<T>, E>>,
    Q: Drive<Svc::Future>,
    E: Into<BoxDynError>,
{
    type Item = Result<Option<Svc::Response>, CallAllError<Svc::Error>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            // First, see if we have any responses to yield
            if let Poll::Ready(Some(result)) = this.queue.poll(cx) {
                return Poll::Ready(Some(result.map_err(CallAllError::ServiceError).map(Some)));
            }

            // If there are no more requests coming, check if we're done
            if *this.eof {
                if this.queue.is_empty() {
                    return Poll::Ready(None);
                } else {
                    return Poll::Pending;
                }
            }

            // Then, see that the service is ready for another request
            let svc = this
                .service
                .as_mut()
                .expect("Using CallAll after extracting inner Service");

            if let Err(e) = ready!(svc.poll_ready(cx)) {
                // Set eof to prevent the service from being called again after a `poll_ready` error
                *this.eof = true;
                return Poll::Ready(Some(Err(CallAllError::ServiceError(e))));
            }

            // If not done, and we don't have a stored request, gather the next request from the
            // stream (if there is one), or return `Pending` if the stream is not ready.
            if this.curr_req.is_none() {
                match ready!(this.stream.as_mut().poll_next(cx)) {
                    Some(Ok(Some(next_req))) => {
                        *this.curr_req = Some(next_req);
                    }
                    Some(Ok(None)) => {
                        return Poll::Ready(Some(Ok(None)));
                    }
                    Some(Err(e)) => {
                        // Stream error, propagate it
                        return Poll::Ready(Some(Err(CallAllError::StreamError(e.into()))));
                    }
                    None => {
                        // Stream is exhausted, mark EOF
                        *this.eof = true;
                        continue;
                    }
                }
            }

            // Unwrap: The check above always sets `this.curr_req` if none and continues if no request.
            this.queue.push(svc.call(this.curr_req.take().unwrap()));
        }
    }
}
