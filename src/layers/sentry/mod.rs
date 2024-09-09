use sentry_core::protocol;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::Layer;
use tower::Service;

use apalis_core::error::Error;
use apalis_core::request::Request;
use apalis_core::task::task_id::TaskId;

/// Tower Layer that logs Job Details.
///
/// The Service created by this Layer can also optionally start a new
/// performance monitoring transaction for each incoming request,
/// continuing the trace based on incoming distributed tracing headers.
///
/// The created transaction will automatically use `J::NAME` as its name.
/// This is sometimes not desirable, In which case, users should manually override the transaction name
/// in the request handler using the [`Scope::set_transaction`](sentry_core::Scope::set_transaction)
/// method.
#[derive(Clone, Default, Debug)]
pub struct SentryLayer;

impl SentryLayer {
    /// Creates a new Layer that only logs Job details.
    pub fn new() -> Self {
        Self
    }
}

/// Tower Service that logs Job details.
///
/// The Service can also optionally start a new performance monitoring transaction
/// for each incoming request, continuing the trace based on J::NAME
#[derive(Clone, Debug)]
pub struct SentryJobService<S> {
    service: S,
}

impl<S> Layer<S> for SentryLayer {
    type Service = SentryJobService<S>;

    fn layer(&self, service: S) -> Self::Service {
        Self::Service { service }
    }
}

struct Task {
    id: TaskId,
    current_attempt: i32,
    namespace: String,
}

pin_project_lite::pin_project! {
    /// The Future returned from [`SentryJobService`].
    pub struct SentryHttpFuture<F> {
        on_first_poll: Option<(
            Task,
            sentry_core::TransactionContext
        )>,
        transaction: Option<(
            sentry_core::TransactionOrSpan,
            Option<sentry_core::TransactionOrSpan>,
        )>,
        #[pin]
        future: F,
    }
}

impl<F, Res> Future for SentryHttpFuture<F>
where
    F: Future<Output = Result<Res, Error>> + 'static,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let slf = self.project();
        if let Some((job_details, trx_ctx)) = slf.on_first_poll.take() {
            let jid = job_details.id.clone();
            sentry_core::configure_scope(|scope| {
                scope.add_event_processor(move |mut event| {
                    event.event_id = uuid::Uuid::from_u128(jid.inner().0);
                    Some(event)
                });
                scope.set_tag("namespace", job_details.namespace.to_string());
                let mut details = std::collections::BTreeMap::new();
                details.insert(String::from("task_id"), job_details.id.to_string().into());
                details.insert(
                    String::from("current_attempt"),
                    job_details.current_attempt.into(),
                );
                scope.set_context("task", sentry_core::protocol::Context::Other(details));

                let transaction: sentry_core::TransactionOrSpan =
                    sentry_core::start_transaction(trx_ctx).into();
                let parent_span = scope.get_span();
                scope.set_span(Some(transaction.clone()));
                *slf.transaction = Some((transaction, parent_span));
            });
        }
        match slf.future.poll(cx) {
            Poll::Ready(res) => {
                if let Some((transaction, parent_span)) = slf.transaction.take() {
                    if transaction.get_status().is_none() {
                        let status = match &res {
                            Ok(_) => protocol::SpanStatus::Ok,
                            Err(err) => {
                                sentry_core::capture_error(err);
                                protocol::SpanStatus::InternalError
                            }
                        };
                        transaction.set_status(status);
                    }
                    transaction.finish();
                    sentry_core::configure_scope(|scope| scope.set_span(parent_span));
                }
                Poll::Ready(res)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<Svc, Req, Ctx, Fut, Res> Service<Request<Req, Ctx>> for SentryJobService<Svc>
where
    Svc: Service<Request<Req, Ctx>, Response = Res, Error = Error, Future = Fut>,
    Fut: Future<Output = Result<Res, Error>> + 'static,
{
    type Response = Svc::Response;
    type Error = Svc::Error;
    type Future = SentryHttpFuture<Svc::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Req, Ctx>) -> Self::Future {
        let task_type = std::any::type_name::<Req>().to_string();
        let attempt = &request.parts.attempt;
        let task_id = &request.parts.task_id;
        let namespace = request
            .parts
            .namespace
            .as_ref()
            .map(|s| s.0.as_str())
            .unwrap_or(std::any::type_name::<Req>());
        let trx_ctx = sentry_core::TransactionContext::new(namespace, "apalis.task");

        let task_details = Task {
            id: task_id.clone(),
            current_attempt: attempt.current().try_into().unwrap(),
            namespace: task_type,
        };

        SentryHttpFuture {
            on_first_poll: Some((task_details, trx_ctx)),
            transaction: None,
            future: self.service.call(request),
        }
    }
}
