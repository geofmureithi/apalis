use futures::{ready, stream::FuturesUnordered, Stream};
use pin_project_lite::pin_project;
use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tower::Service;

pin_project! {
    /// A stream of responses received from the inner service in received order.
    #[derive(Debug)]
    pub(super) struct CallAllUnordered<Svc, S>
    where
        Svc: Service<S::Item>,
        S: Stream,
    {
        #[pin]
        inner: CallAll<Svc, S, FuturesUnordered<Svc::Future>>,
    }
}

impl<Svc, S> CallAllUnordered<Svc, S>
where
    Svc: Service<S::Item>,
    S: Stream,
{
    /// Create new [`CallAllUnordered`] combinator.
    ///
    /// [`Stream`]: https://docs.rs/futures/latest/futures/stream/trait.Stream.html
    pub(super) fn new(service: Svc, stream: S) -> CallAllUnordered<Svc, S> {
        CallAllUnordered {
            inner: CallAll::new(service, stream, FuturesUnordered::new()),
        }
    }
}

impl<Svc, S> Stream for CallAllUnordered<Svc, S>
where
    Svc: Service<S::Item>,
    S: Stream,
{
    type Item = Result<Svc::Response, Svc::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
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

pin_project! {
    /// The [`Future`] returned by the [`ServiceExt::call_all`] combinator.
    pub(crate) struct CallAll<Svc, S, Q>
    where
        S: Stream,
    {
        service: Option<Svc>,
        #[pin]
        stream: S,
        queue: Q,
        eof: bool,
        curr_req: Option<S::Item>
    }
}

impl<Svc, S, Q> fmt::Debug for CallAll<Svc, S, Q>
where
    Svc: fmt::Debug,
    S: Stream + fmt::Debug,
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

impl<Svc, S, Q> CallAll<Svc, S, Q>
where
    Svc: Service<S::Item>,
    S: Stream,
    Q: Drive<Svc::Future>,
{
    pub(crate) const fn new(service: Svc, stream: S, queue: Q) -> CallAll<Svc, S, Q> {
        CallAll {
            service: Some(service),
            stream,
            queue,
            eof: false,
            curr_req: None,
        }
    }
}

impl<Svc, S, Q> Stream for CallAll<Svc, S, Q>
where
    Svc: Service<S::Item>,
    S: Stream,
    Q: Drive<Svc::Future>,
{
    type Item = Result<Svc::Response, Svc::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            // First, see if we have any responses to yield
            if let Poll::Ready(r) = this.queue.poll(cx) {
                if let Some(rsp) = r.transpose()? {
                    return Poll::Ready(Some(Ok(rsp)));
                }
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
                return Poll::Ready(Some(Err(e)));
            }

            // If not done, and we don't have a stored request, gather the next request from the
            // stream (if there is one), or return `Pending` if the stream is not ready.
            if this.curr_req.is_none() {
                *this.curr_req = match ready!(this.stream.as_mut().poll_next(cx)) {
                    Some(next_req) => Some(next_req),
                    None => {
                        // Mark that there will be no more requests.
                        *this.eof = true;
                        continue;
                    }
                };
            }

            // Unwrap: The check above always sets `this.curr_req` if none.
            this.queue.push(svc.call(this.curr_req.take().unwrap()));
        }
    }
}
