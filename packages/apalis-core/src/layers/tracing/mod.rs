mod make_span;
mod on_failure;
mod on_request;
mod on_response;

use crate::{error::JobError, request::JobRequest, response::JobResult};
use std::{
    fmt::{self, Debug},
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};
use tower::Service;
use tracing::{Level, Span};

use self::{
    make_span::{DefaultMakeSpan, MakeSpan},
    on_failure::{DefaultOnFailure, OnFailure},
    on_request::{DefaultOnRequest, OnRequest},
    on_response::{DefaultOnResponse, OnResponse},
};
use futures::Future;
use pin_project_lite::pin_project;
use tower::Layer;

const DEFAULT_MESSAGE_LEVEL: Level = Level::DEBUG;
const DEFAULT_ERROR_LEVEL: Level = Level::ERROR;

/// The latency unit used to report latencies.
#[non_exhaustive]
#[derive(Copy, Clone, Debug)]
pub enum LatencyUnit {
    // /// Use minutes.
    // Minutes,
    /// Use seconds.
    Seconds,
    /// Use milliseconds.
    Millis,
    /// Use microseconds.
    Micros,
    /// Use nanoseconds.
    Nanos,
}

/// [`Layer`] that adds high level [tracing] to a [`Service`].
///
/// See the [module docs](crate::trace) for more details.
///
/// [`Layer`]: tower_layer::Layer
/// [tracing]: https://crates.io/crates/tracing
/// [`Service`]: tower_service::Service
#[derive(Debug, Copy, Clone)]
pub struct TraceLayer<
    MakeSpan = DefaultMakeSpan,
    OnRequest = DefaultOnRequest,
    OnResponse = DefaultOnResponse,
    OnFailure = DefaultOnFailure,
> {
    pub(crate) make_span: MakeSpan,
    pub(crate) on_request: OnRequest,
    pub(crate) on_response: OnResponse,
    pub(crate) on_failure: OnFailure,
}

impl TraceLayer {
    /// Create a new [`TraceLayer`].
    pub fn new() -> Self {
        Self {
            make_span: DefaultMakeSpan::new(),
            on_failure: DefaultOnFailure::default(),
            on_request: DefaultOnRequest::default(),
            on_response: DefaultOnResponse::default(),
        }
    }
}

impl<MakeSpan, OnRequest, OnResponse, OnFailure>
    TraceLayer<MakeSpan, OnRequest, OnResponse, OnFailure>
{
    /// Customize what to do when a request is received.
    ///
    /// `NewOnRequest` is expected to implement [`OnRequest`].
    ///
    /// [`OnRequest`]: super::OnRequest
    pub fn on_request<NewOnRequest>(
        self,
        new_on_request: NewOnRequest,
    ) -> TraceLayer<MakeSpan, NewOnRequest, OnResponse, OnFailure> {
        TraceLayer {
            on_request: new_on_request,
            on_failure: self.on_failure,
            make_span: self.make_span,
            on_response: self.on_response,
        }
    }

    /// Customize what to do when a response has been produced.
    ///
    /// `NewOnResponse` is expected to implement [`OnResponse`].
    ///
    /// [`OnResponse`]: super::OnResponse
    pub fn on_response<NewOnResponse>(
        self,
        new_on_response: NewOnResponse,
    ) -> TraceLayer<MakeSpan, OnRequest, NewOnResponse, OnFailure> {
        TraceLayer {
            on_response: new_on_response,
            on_request: self.on_request,
            on_failure: self.on_failure,
            make_span: self.make_span,
        }
    }

    /// Customize what to do when a response has been classified as a failure.
    ///
    /// `NewOnFailure` is expected to implement [`OnFailure`].
    ///
    /// [`OnFailure`]: super::OnFailure
    pub fn on_failure<NewOnFailure>(
        self,
        new_on_failure: NewOnFailure,
    ) -> TraceLayer<MakeSpan, OnRequest, OnResponse, NewOnFailure> {
        TraceLayer {
            on_failure: new_on_failure,
            on_request: self.on_request,

            make_span: self.make_span,
            on_response: self.on_response,
        }
    }

    /// Customize how to make [`Span`]s that all request handling will be wrapped in.
    ///
    /// `NewMakeSpan` is expected to implement [`MakeSpan`].
    ///
    /// [`MakeSpan`]: super::MakeSpan
    /// [`Span`]: tracing::Span
    pub fn make_span_with<NewMakeSpan>(
        self,
        new_make_span: NewMakeSpan,
    ) -> TraceLayer<NewMakeSpan, OnRequest, OnResponse, OnFailure> {
        TraceLayer {
            make_span: new_make_span,
            on_request: self.on_request,
            on_failure: self.on_failure,

            on_response: self.on_response,
        }
    }
}

impl<S, MakeSpan, OnRequest, OnResponse, OnFailure> Layer<S>
    for TraceLayer<MakeSpan, OnRequest, OnResponse, OnFailure>
where
    MakeSpan: Clone,
    OnRequest: Clone,
    OnResponse: Clone,
    OnFailure: Clone,
{
    type Service = Trace<S, MakeSpan, OnRequest, OnResponse, OnFailure>;

    fn layer(&self, inner: S) -> Self::Service {
        Trace {
            inner,
            make_span: self.make_span.clone(),
            on_request: self.on_request.clone(),
            on_response: self.on_response.clone(),
            on_failure: self.on_failure.clone(),
        }
    }
}

/// Middleware that adds high level [tracing] to a [`Service`].
///
/// See the [module docs](crate::trace) for an example.
///
/// [tracing]: https://crates.io/crates/tracing
/// [`Service`]: tower_service::Service
#[derive(Debug, Clone, Copy)]
pub struct Trace<
    S,
    MakeSpan = DefaultMakeSpan,
    OnRequest = DefaultOnRequest,
    OnResponse = DefaultOnResponse,
    OnFailure = DefaultOnFailure,
> {
    pub(crate) inner: S,

    pub(crate) make_span: MakeSpan,
    pub(crate) on_request: OnRequest,
    pub(crate) on_response: OnResponse,

    pub(crate) on_failure: OnFailure,
}

impl<S> Trace<S> {
    /// Create a new [`Trace`] .
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            make_span: DefaultMakeSpan::new(),
            on_request: DefaultOnRequest::default(),
            on_response: DefaultOnResponse::default(),
            on_failure: DefaultOnFailure::default(),
        }
    }

    /// Returns a new [`Layer`] that wraps services with a [`TraceLayer`] middleware.
    ///
    /// [`Layer`]: tower_layer::Layer
    pub fn layer() -> TraceLayer {
        TraceLayer::new()
    }
}

impl<S, MakeSpan, OnRequest, OnResponse, OnFailure>
    Trace<S, MakeSpan, OnRequest, OnResponse, OnFailure>
{
    /// Gets a reference to the underlying service.
    pub fn get_ref(&self) -> &S {
        &self.inner
    }

    /// Gets a mutable reference to the underlying service.
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// Consumes `self`, returning the underlying service.
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// Customize what to do when a request is received.
    ///
    /// `NewOnRequest` is expected to implement [`OnRequest`].
    ///
    /// [`OnRequest`]: super::OnRequest
    pub fn on_request<NewOnRequest>(
        self,
        new_on_request: NewOnRequest,
    ) -> Trace<S, MakeSpan, NewOnRequest, OnResponse, OnFailure> {
        Trace {
            on_request: new_on_request,
            inner: self.inner,
            on_failure: self.on_failure,
            make_span: self.make_span,
            on_response: self.on_response,
        }
    }

    /// Customize what to do when a response has been produced.
    ///
    /// `NewOnResponse` is expected to implement [`OnResponse`].
    ///
    /// [`OnResponse`]: super::OnResponse
    pub fn on_response<NewOnResponse>(
        self,
        new_on_response: NewOnResponse,
    ) -> Trace<S, MakeSpan, OnRequest, NewOnResponse, OnFailure> {
        Trace {
            on_response: new_on_response,
            inner: self.inner,
            on_request: self.on_request,
            on_failure: self.on_failure,
            make_span: self.make_span,
        }
    }

    /// Customize what to do when a response has been classified as a failure.
    ///
    /// `NewOnFailure` is expected to implement [`OnFailure`].
    ///
    /// [`OnFailure`]: super::OnFailure
    pub fn on_failure<NewOnFailure>(
        self,
        new_on_failure: NewOnFailure,
    ) -> Trace<S, MakeSpan, OnRequest, OnResponse, NewOnFailure> {
        Trace {
            on_failure: new_on_failure,
            inner: self.inner,
            make_span: self.make_span,
            on_request: self.on_request,
            on_response: self.on_response,
        }
    }

    /// Customize how to make [`Span`]s that all request handling will be wrapped in.
    ///
    /// `NewMakeSpan` is expected to implement [`MakeSpan`].
    ///
    /// [`MakeSpan`]: super::MakeSpan
    /// [`Span`]: tracing::Span
    pub fn make_span_with<NewMakeSpan>(
        self,
        new_make_span: NewMakeSpan,
    ) -> Trace<S, NewMakeSpan, OnRequest, OnResponse, OnFailure> {
        Trace {
            make_span: new_make_span,
            inner: self.inner,
            on_failure: self.on_failure,
            on_request: self.on_request,
            on_response: self.on_response,
        }
    }
}

impl<J, S, OnRequestT, OnResponseT, OnFailureT, MakeSpanT, F> Service<JobRequest<J>>
    for Trace<S, MakeSpanT, OnRequestT, OnResponseT, OnFailureT>
where
    S: Service<JobRequest<J>, Response = JobResult, Error = JobError, Future = F>
        + Unpin
        + Send
        + 'static,
    S::Error: fmt::Display + 'static,
    MakeSpanT: MakeSpan<J>,
    OnRequestT: OnRequest<J>,
    OnResponseT: OnResponse + Clone + 'static,
    F: Future<Output = Result<JobResult, JobError>> + 'static,
    OnFailureT: OnFailure + Clone + 'static,
{
    type Response = JobResult;
    type Error = JobError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: JobRequest<J>) -> Self::Future {
        let span = self.make_span.make_span(&req);
        let start = Instant::now();
        let job = {
            let _guard = span.enter();
            self.on_request.on_request(&req, &span);
            self.inner.call(req)
        };

        let future = ResponseFuture {
            inner: job,
            span,
            on_response: Some(self.on_response.clone()),
            on_failure: Some(self.on_failure.clone()),
            start,
        };

        Box::pin(future)
    }
}

pin_project! {
    struct ResponseFuture<F, OnResponse, OnFailure> {
        #[pin]
        pub(crate) inner: F,
        pub(crate) span: Span,
        pub(crate) on_response: Option<OnResponse>,
        pub(crate) on_failure: Option<OnFailure>,
        pub(crate) start: Instant,
    }
}

impl<Fut, OnResponseT, OnFailureT> Future for ResponseFuture<Fut, OnResponseT, OnFailureT>
where
    Fut: Future<Output = Result<JobResult, JobError>>,

    OnResponseT: OnResponse,
    OnFailureT: OnFailure,
{
    type Output = Result<JobResult, JobError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let _guard = this.span.enter();
        let result = futures::ready!(this.inner.poll(cx));
        let done_in = this.start.elapsed();
        let mut on_failure = this.on_failure.take().unwrap();

        match result {
            Ok(res) => {
                this.on_response
                    .take()
                    .unwrap()
                    .on_response(&res, done_in, this.span);

                Poll::Ready(Ok(res))
            }
            Err(err) => {
                on_failure.on_failure(&err, done_in, this.span);
                Poll::Ready(Err(err))
            }
        }
    }
}
