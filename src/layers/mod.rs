/// Prometheus integration for apalis
#[cfg(feature = "prometheus")]
#[cfg_attr(docsrs, doc(cfg(feature = "prometheus")))]
pub mod prometheus;
/// Retry job middleware
#[cfg(feature = "retry")]
#[cfg_attr(docsrs, doc(cfg(feature = "retry")))]
pub mod retry;
/// Sentry integration for apalis.
#[cfg(feature = "sentry")]
#[cfg_attr(docsrs, doc(cfg(feature = "sentry")))]
pub mod sentry;
/// Tracing integration for apalis
#[cfg(feature = "tracing")]
#[cfg_attr(docsrs, doc(cfg(feature = "tracing")))]
pub mod tracing;
/// Rate limit middleware for apalis
#[cfg(feature = "limit")]
#[cfg_attr(docsrs, doc(cfg(feature = "limit")))]
pub mod limit {
    pub use tower::limit::ConcurrencyLimitLayer;
    pub use tower::limit::GlobalConcurrencyLimitLayer;
    pub use tower::limit::RateLimitLayer;
}

use apalis_core::worker::builder::WorkerBuilder;
#[cfg(feature = "catch-panic")]
use catch_panic::CatchPanicLayer;
use tower::layer::util::{Identity, Stack};
/// Timeout middleware for apalis
#[cfg(feature = "timeout")]
#[cfg_attr(docsrs, doc(cfg(feature = "timeout")))]
pub use tower::timeout::TimeoutLayer;

/// catch panic middleware for apalis
#[cfg(feature = "catch-panic")]
#[cfg_attr(docsrs, doc(cfg(feature = "catch-panic")))]
pub mod catch_panic;

/// A trait that extends `WorkerBuilder` with additional middleware methods
/// derived from `tower::ServiceBuilder`.
pub trait WorkerBuilderExt<Req, Source, Middleware> {
    /// Optionally adds a new layer `T` into the [`WorkerBuilder`].
    fn option_layer<T>(
        self,
        layer: Option<T>,
    ) -> WorkerBuilder<Req, Source, Stack<tower::util::Either<T, Identity>, Middleware>>;

    /// Adds a [`Layer`] built from a function that accepts a service and returns another service.
    fn layer_fn<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Source, Stack<tower::layer::LayerFn<F>, Middleware>>;

    /// Limits the max number of in-flight requests.
    #[cfg(feature = "limit")]
    fn concurrency(
        self,
        max: usize,
    ) -> WorkerBuilder<
        Req,
        Source,
        Srv,
        Stack<tower::limit::ConcurrencyLimitLayer, Middleware>,
        Worker,
    >;

    /// Limits requests to at most `num` per the given duration.
    #[cfg(feature = "limit")]
    fn rate_limit(
        self,
        num: u64,
        per: std::time::Duration,
    ) -> WorkerBuilder<Req, Source, Stack<tower::limit::RateLimitLayer, Middleware>>;

    /// Retries failed requests according to the given retry policy.
    #[cfg(feature = "retry")]
    fn retry<P>(
        self,
        policy: P,
    ) -> WorkerBuilder<Req, Source, Stack<tower::retry::RetryLayer<P>, Middleware>>;

    /// Fails requests that take longer than `timeout`.
    #[cfg(feature = "timeout")]
    fn timeout(
        self,
        timeout: std::time::Duration,
    ) -> WorkerBuilder<Req, Source, Stack<tower::timeout::TimeoutLayer, Middleware>>;

    /// Conditionally rejects requests based on `predicate`.
    #[cfg(feature = "filter")]
    fn filter<P>(
        self,
        predicate: P,
    ) -> WorkerBuilder<Req, Source, Stack<tower::filter::FilterLayer<P>, Middleware>>;

    /// Conditionally rejects requests based on an asynchronous `predicate`.
    #[cfg(feature = "filter")]
    fn filter_async<P>(
        self,
        predicate: P,
    ) -> WorkerBuilder<
        Req,
        Source,
        Srv,
        Stack<tower::filter::AsyncFilterLayer<P>, Middleware>,
        Worker,
    >;

    /// Maps one request type to another.
    fn map_request<F, R1, R2>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Source, Stack<tower::util::MapRequestLayer<F>, Middleware>>
    where
        F: FnMut(R1) -> R2 + Clone;

    /// Maps one response type to another.
    fn map_response<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Source, Stack<tower::util::MapResponseLayer<F>, Middleware>>;

    /// Maps one error type to another.
    fn map_err<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Source, Stack<tower::util::MapErrLayer<F>, Middleware>>;

    /// Composes a function that transforms futures produced by the service.
    fn map_future<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Source, Stack<tower::util::MapFutureLayer<F>, Middleware>>;

    /// Applies an asynchronous function after the service, regardless of whether the future succeeds or fails.
    fn then<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Source, Stack<tower::util::ThenLayer<F>, Middleware>>;

    /// Executes a new future after this service's future resolves.
    fn and_then<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Source, Stack<tower::util::AndThenLayer<F>, Middleware>>;

    /// Maps the service's result type to a different value, regardless of success or failure.
    fn map_result<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Source, Stack<tower::util::MapResultLayer<F>, Middleware>>;

    /// Catch panics in execution and pipe them as errors
    #[cfg(feature = "catch-panic")]
    #[cfg_attr(docsrs, doc(cfg(feature = "catch-panic")))]
    #[allow(clippy::type_complexity)]
    fn catch_panic(
        self,
    ) -> WorkerBuilder<
        Req,
        Ctx,
        Source,
        Stack<
            CatchPanicLayer<fn(Box<dyn std::any::Any + Send>) -> apalis_core::error::Error>,
            Middleware,
        >,
        Worker,
    >;
    /// Enable tracing via tracing crate
    #[cfg(feature = "tracing")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tracing")))]
    fn enable_tracing(self) -> WorkerBuilder<Req, Source, Stack<tracing::TraceLayer, Middleware>>;
}

impl<Req, Middleware, Source> WorkerBuilderExt<Req, Source, Middleware>
    for WorkerBuilder<Req, Source, Middleware>
{
    fn option_layer<T>(
        self,
        layer: Option<T>,
    ) -> WorkerBuilder<Req, Source, Stack<tower::util::Either<T, Identity>, Middleware>> {
        self.chain(|sb| sb.option_layer(layer))
    }

    fn layer_fn<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Source, Stack<tower::layer::LayerFn<F>, Middleware>> {
        self.chain(|sb| sb.layer_fn(f))
    }

    #[cfg(feature = "limit")]
    fn concurrency(
        self,
        max: usize,
    ) -> WorkerBuilder<
        Req,
        Source,
        Svc,
        Stack<tower::limit::ConcurrencyLimitLayer, Middleware>,
        Worker,
    > {
        self.chain(|sb| sb.concurrency_limit(max))
    }

    #[cfg(feature = "limit")]
    fn rate_limit(
        self,
        num: u64,
        per: std::time::Duration,
    ) -> WorkerBuilder<Req, Source, Stack<tower::limit::RateLimitLayer, Middleware>> {
        self.chain(|sb| sb.rate_limit(num, per))
    }

    #[cfg(feature = "retry")]
    fn retry<P>(
        self,
        policy: P,
    ) -> WorkerBuilder<Req, Source, Stack<tower::retry::RetryLayer<P>, Middleware>> {
        self.chain(|sb| sb.retry(policy))
    }

    #[cfg(feature = "timeout")]
    fn timeout(
        self,
        timeout: std::time::Duration,
    ) -> WorkerBuilder<Req, Source, Stack<tower::timeout::TimeoutLayer, Middleware>> {
        self.chain(|sb| sb.timeout(timeout))
    }

    #[cfg(feature = "filter")]
    fn filter<P>(
        self,
        predicate: P,
    ) -> WorkerBuilder<Req, Source, Stack<tower::filter::FilterLayer<P>, Middleware>> {
        self.chain(|sb| sb.filter(predicate))
    }

    #[cfg(feature = "filter")]
    fn filter_async<P>(
        self,
        predicate: P,
    ) -> WorkerBuilder<
        Req,
        Source,
        Svc,
        Stack<tower::filter::AsyncFilterLayer<P>, Middleware>,
        Worker,
    > {
        self.chain(|sb| sb.filter_async(predicate))
    }

    fn map_request<F, R1, R2>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Source, Stack<tower::util::MapRequestLayer<F>, Middleware>>
    where
        F: FnMut(R1) -> R2 + Clone,
    {
        self.chain(|sb| sb.map_request(f))
    }

    fn map_response<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Source, Stack<tower::util::MapResponseLayer<F>, Middleware>> {
        self.chain(|sb| sb.map_response(f))
    }

    fn map_err<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Source, Stack<tower::util::MapErrLayer<F>, Middleware>> {
        self.chain(|sb| sb.map_err(f))
    }

    fn map_future<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Source, Stack<tower::util::MapFutureLayer<F>, Middleware>> {
        self.chain(|sb| sb.map_future(f))
    }

    fn then<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Source, Stack<tower::util::ThenLayer<F>, Middleware>> {
        self.chain(|sb| sb.then(f))
    }

    fn and_then<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Source, Stack<tower::util::AndThenLayer<F>, Middleware>> {
        self.chain(|sb| sb.and_then(f))
    }

    fn map_result<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Source, Stack<tower::util::MapResultLayer<F>, Middleware>> {
        self.chain(|sb| sb.map_result(f))
    }

    /// Catch panics in execution and pipe them as errors
    #[cfg(feature = "catch-panic")]
    #[cfg_attr(docsrs, doc(cfg(feature = "catch-panic")))]
    fn catch_panic(
        self,
    ) -> WorkerBuilder<
        Req,
        Source,
        Svc,
        Stack<
            CatchPanicLayer<fn(Box<dyn std::any::Any + Send>) -> apalis_core::error::Error>,
            Middleware,
        >,
        Worker,
    > {
        self.chain(|svc| svc.layer(CatchPanicLayer::new()))
    }

    /// Enable tracing via tracing crate
    #[cfg(feature = "tracing")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tracing")))]
    fn enable_tracing(self) -> WorkerBuilder<Req, Source, Stack<tracing::TraceLayer, Middleware>> {
        use tracing::TraceLayer;

        self.chain(|svc| svc.layer(TraceLayer::new()))
    }
}
