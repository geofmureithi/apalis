use sentry_core::protocol;
use std::fmt::{self, Debug};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::Layer;
use tower::Service;

use apalis_core::task::Task;
use apalis_core::task::task_id::RandomId;

/// Sentry integration Layer.
///
/// The Service created by this Layer can also optionally start a new
/// performance monitoring transaction for each incoming task,
/// continuing the trace based on incoming distributed tracing parts.
///
/// This is sometimes not desirable, In which case, users should manually override the transaction name
/// in the task handler using the [`Scope::set_transaction`](sentry_core::Scope::set_transaction)
/// method.
#[derive(Clone, Default, Debug)]
pub struct SentryLayer;

impl SentryLayer {
    /// Creates a new Layer that only logs task details.
    pub fn new() -> Self {
        Self
    }
}

/// Task Service for Sentry integration.
///
/// The Service can also optionally start a new performance monitoring transaction
/// for each incoming task, continuing the trace based on the task type.
#[derive(Clone, Debug)]
pub struct SentryTaskService<S> {
    service: S,
}

impl<S> Layer<S> for SentryLayer {
    type Service = SentryTaskService<S>;

    fn layer(&self, service: S) -> Self::Service {
        Self::Service { service }
    }
}

struct Request {
    id: uuid::Uuid,
    current_attempt: i32,
    namespace: String,
}

/// The Future returned from [`SentryTaskService`].
#[pin_project::pin_project]
pub struct SentryHttpFuture<F> {
    on_first_poll: Option<(Request, sentry_core::TransactionContext)>,
    transaction: Option<(
        sentry_core::TransactionOrSpan,
        Option<sentry_core::TransactionOrSpan>,
    )>,
    #[pin]
    future: F,
}

impl<F> fmt::Debug for SentryHttpFuture<F>
where
    F: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SentryHttpFuture")
            .field("on_first_poll", &"<Request, TransactionContext>")
            .field(
                "transaction",
                &self.transaction.as_ref().map(|(_, maybe_span)| {
                    let has_child = maybe_span.is_some();
                    format!(
                        "<TransactionOrSpan, child: {}>",
                        if has_child { "Some" } else { "None" }
                    )
                }),
            )
            .field("future", &self.future)
            .finish()
    }
}

impl<F, Res, Err> Future for SentryHttpFuture<F>
where
    F: Future<Output = Result<Res, Err>> + 'static,
    Err: std::error::Error,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let slf = self.project();
        if let Some((task_details, trx_ctx)) = slf.on_first_poll.take() {
            let tid = task_details.id.clone();
            sentry_core::configure_scope(|scope| {
                scope.add_event_processor(move |mut event| {
                    event.event_id = tid;
                    Some(event)
                });
                scope.set_tag("namespace", task_details.namespace.to_string());
                let mut details = std::collections::BTreeMap::new();
                details.insert(String::from("task_id"), task_details.id.to_string().into());
                details.insert(
                    String::from("current_attempt"),
                    task_details.current_attempt.into(),
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

impl<Svc, Args, Ctx, Fut, Res, IdType, Err> Service<Task<Args, Ctx, IdType>>
    for SentryTaskService<Svc>
where
    Svc: Service<Task<Args, Ctx, IdType>, Response = Res, Error = Err, Future = Fut>,
    Fut: Future<Output = Result<Res, Err>> + 'static,
    IdType: ToUuid,
    Err: std::error::Error,
{
    type Response = Svc::Response;
    type Error = Svc::Error;
    type Future = SentryHttpFuture<Svc::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, task: Task<Args, Ctx, IdType>) -> Self::Future {
        let task_type = std::any::type_name::<Args>().to_string();
        let attempt = &task.parts.attempt;
        let task_id = task
            .parts
            .task_id
            .as_ref()
            .expect("Task ID is missing")
            .inner()
            .to_uuid();
        let trx_ctx =
            sentry_core::TransactionContext::new(std::any::type_name::<Args>(), "apalis.task");

        let task_details = Request {
            id: task_id.clone(),
            current_attempt: attempt.current().try_into().unwrap_or_default(),
            namespace: task_type,
        };

        SentryHttpFuture {
            on_first_poll: Some((task_details, trx_ctx)),
            transaction: None,
            future: self.service.call(task),
        }
    }
}
/// Trait for converting types to UUIDs.
pub trait ToUuid {
    /// Converts the implementing type to a UUID.
    fn to_uuid(&self) -> uuid::Uuid;
}

impl ToUuid for uuid::Uuid {
    fn to_uuid(&self) -> uuid::Uuid {
        *self
    }
}

impl ToUuid for String {
    fn to_uuid(&self) -> uuid::Uuid {
        uuid::Uuid::parse_str(self).expect("Not a valid UUID")
    }
}

impl ToUuid for &str {
    fn to_uuid(&self) -> uuid::Uuid {
        uuid::Uuid::parse_str(self).expect("Not a valid UUID")
    }
}
impl ToUuid for ulid::Ulid {
    fn to_uuid(&self) -> uuid::Uuid {
        uuid::Uuid::from_u128(self.0)
    }
}

impl ToUuid for RandomId {
    fn to_uuid(&self) -> uuid::Uuid {
        use std::hash::DefaultHasher;
        use std::hash::Hash;
        use std::hash::Hasher;
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        let hash = hasher.finish();

        // Expand to 128 bits by hashing again with a different salt
        let mut hasher2 = DefaultHasher::new();
        (hash, 0xDEADBEEFu32 as i32).hash(&mut hasher2);
        let hash2 = hasher2.finish();

        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&hash.to_be_bytes());
        bytes[8..].copy_from_slice(&hash2.to_be_bytes());

        // Set version (v4 style) and variant bits to make it a valid UUID
        bytes[6] = (bytes[6] & 0x0F) | 0x40; // Version 4
        bytes[8] = (bytes[8] & 0x3F) | 0x80; // Variant RFC 4122

        uuid::Uuid::from_bytes(bytes)
    }
}
