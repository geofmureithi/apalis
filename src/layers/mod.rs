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

use apalis_core::{builder::WorkerBuilder, layers::Identity};
#[cfg(feature = "catch-panic")]
use catch_panic::CatchPanicLayer;
use tower::layer::util::Stack;
/// Timeout middleware for apalis
#[cfg(feature = "timeout")]
#[cfg_attr(docsrs, doc(cfg(feature = "timeout")))]
pub use tower::timeout::TimeoutLayer;

/// catch panic middleware for apalis
#[cfg(feature = "catch-panic")]
#[cfg_attr(docsrs, doc(cfg(feature = "catch-panic")))]
pub mod catch_panic;

pub use apalis_core::error::ErrorHandlingLayer;

/// A trait that extends `WorkerBuilder` with additional middleware methods
/// derived from `tower::ServiceBuilder`.
pub trait WorkerBuilderExt<Req, Ctx, Source, Middleware, Serv> {
    /// Optionally adds a new layer `T` into the [`WorkerBuilder`].
    fn option_layer<T>(
        self,
        layer: Option<T>,
    ) -> WorkerBuilder<Req, Ctx, Source, Stack<tower::util::Either<T, Identity>, Middleware>, Serv>;

    /// Adds a [`Layer`] built from a function that accepts a service and returns another service.
    fn layer_fn<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Ctx, Source, Stack<tower::layer::LayerFn<F>, Middleware>, Serv>;

    /// Limits the max number of in-flight requests.
    #[cfg(feature = "limit")]
    fn concurrency(
        self,
        max: usize,
    ) -> WorkerBuilder<Req, Ctx, Source, Stack<tower::limit::ConcurrencyLimitLayer, Middleware>, Serv>;

    /// Limits requests to at most `num` per the given duration.
    #[cfg(feature = "limit")]
    fn rate_limit(
        self,
        num: u64,
        per: std::time::Duration,
    ) -> WorkerBuilder<Req, Ctx, Source, Stack<tower::limit::RateLimitLayer, Middleware>, Serv>;

    /// Retries failed requests according to the given retry policy.
    #[cfg(feature = "retry")]
    fn retry<P>(
        self,
        policy: P,
    ) -> WorkerBuilder<Req, Ctx, Source, Stack<tower::retry::RetryLayer<P>, Middleware>, Serv>;

    /// Fails requests that take longer than `timeout`.
    #[cfg(feature = "timeout")]
    fn timeout(
        self,
        timeout: std::time::Duration,
    ) -> WorkerBuilder<Req, Ctx, Source, Stack<tower::timeout::TimeoutLayer, Middleware>, Serv>;

    /// Conditionally rejects requests based on `predicate`.
    #[cfg(feature = "filter")]
    fn filter<P>(
        self,
        predicate: P,
    ) -> WorkerBuilder<Req, Ctx, Source, Stack<tower::filter::FilterLayer<P>, Middleware>, Serv>;

    /// Conditionally rejects requests based on an asynchronous `predicate`.
    #[cfg(feature = "filter")]
    fn filter_async<P>(
        self,
        predicate: P,
    ) -> WorkerBuilder<Req, Ctx, Source, Stack<tower::filter::AsyncFilterLayer<P>, Middleware>, Serv>;

    /// Maps one request type to another.
    fn map_request<F, R1, R2>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Ctx, Source, Stack<tower::util::MapRequestLayer<F>, Middleware>, Serv>
    where
        F: FnMut(R1) -> R2 + Clone;

    /// Maps one response type to another.
    fn map_response<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Ctx, Source, Stack<tower::util::MapResponseLayer<F>, Middleware>, Serv>;

    /// Maps one error type to another.
    fn map_err<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Ctx, Source, Stack<tower::util::MapErrLayer<F>, Middleware>, Serv>;

    /// Composes a function that transforms futures produced by the service.
    fn map_future<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Ctx, Source, Stack<tower::util::MapFutureLayer<F>, Middleware>, Serv>;

    /// Applies an asynchronous function after the service, regardless of whether the future succeeds or fails.
    fn then<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Ctx, Source, Stack<tower::util::ThenLayer<F>, Middleware>, Serv>;

    /// Executes a new future after this service's future resolves.
    fn and_then<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Ctx, Source, Stack<tower::util::AndThenLayer<F>, Middleware>, Serv>;

    /// Maps the service's result type to a different value, regardless of success or failure.
    fn map_result<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Ctx, Source, Stack<tower::util::MapResultLayer<F>, Middleware>, Serv>;

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
        Serv,
    >;
    /// Enable tracing via tracing crate
    #[cfg(feature = "tracing")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tracing")))]
    fn enable_tracing(
        self,
    ) -> WorkerBuilder<Req, Ctx, Source, Stack<tracing::TraceLayer, Middleware>, Serv>;
}

impl<Req, Ctx, Middleware, Serv> WorkerBuilderExt<Req, Ctx, (), Middleware, Serv>
    for WorkerBuilder<Req, Ctx, (), Middleware, Serv>
{
    fn option_layer<T>(
        self,
        layer: Option<T>,
    ) -> WorkerBuilder<Req, Ctx, (), Stack<tower::util::Either<T, Identity>, Middleware>, Serv>
    {
        self.chain(|sb| sb.option_layer(layer))
    }

    fn layer_fn<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Ctx, (), Stack<tower::layer::LayerFn<F>, Middleware>, Serv> {
        self.chain(|sb| sb.layer_fn(f))
    }

    #[cfg(feature = "limit")]
    fn concurrency(
        self,
        max: usize,
    ) -> WorkerBuilder<Req, Ctx, (), Stack<tower::limit::ConcurrencyLimitLayer, Middleware>, Serv>
    {
        self.chain(|sb| sb.concurrency_limit(max))
    }

    #[cfg(feature = "limit")]
    fn rate_limit(
        self,
        num: u64,
        per: std::time::Duration,
    ) -> WorkerBuilder<Req, Ctx, (), Stack<tower::limit::RateLimitLayer, Middleware>, Serv> {
        self.chain(|sb| sb.rate_limit(num, per))
    }

    #[cfg(feature = "retry")]
    fn retry<P>(
        self,
        policy: P,
    ) -> WorkerBuilder<Req, Ctx, (), Stack<tower::retry::RetryLayer<P>, Middleware>, Serv> {
        self.chain(|sb| sb.retry(policy))
    }

    #[cfg(feature = "timeout")]
    fn timeout(
        self,
        timeout: std::time::Duration,
    ) -> WorkerBuilder<Req, Ctx, (), Stack<tower::timeout::TimeoutLayer, Middleware>, Serv> {
        self.chain(|sb| sb.timeout(timeout))
    }

    #[cfg(feature = "filter")]
    fn filter<P>(
        self,
        predicate: P,
    ) -> WorkerBuilder<Req, Ctx, (), Stack<tower::filter::FilterLayer<P>, Middleware>, Serv> {
        self.chain(|sb| sb.filter(predicate))
    }

    #[cfg(feature = "filter")]
    fn filter_async<P>(
        self,
        predicate: P,
    ) -> WorkerBuilder<Req, Ctx, (), Stack<tower::filter::AsyncFilterLayer<P>, Middleware>, Serv>
    {
        self.chain(|sb| sb.filter_async(predicate))
    }

    fn map_request<F, R1, R2>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Ctx, (), Stack<tower::util::MapRequestLayer<F>, Middleware>, Serv>
    where
        F: FnMut(R1) -> R2 + Clone,
    {
        self.chain(|sb| sb.map_request(f))
    }

    fn map_response<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Ctx, (), Stack<tower::util::MapResponseLayer<F>, Middleware>, Serv>
    {
        self.chain(|sb| sb.map_response(f))
    }

    fn map_err<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Ctx, (), Stack<tower::util::MapErrLayer<F>, Middleware>, Serv> {
        self.chain(|sb| sb.map_err(f))
    }

    fn map_future<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Ctx, (), Stack<tower::util::MapFutureLayer<F>, Middleware>, Serv> {
        self.chain(|sb| sb.map_future(f))
    }

    fn then<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Ctx, (), Stack<tower::util::ThenLayer<F>, Middleware>, Serv> {
        self.chain(|sb| sb.then(f))
    }

    fn and_then<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Ctx, (), Stack<tower::util::AndThenLayer<F>, Middleware>, Serv> {
        self.chain(|sb| sb.and_then(f))
    }

    fn map_result<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Req, Ctx, (), Stack<tower::util::MapResultLayer<F>, Middleware>, Serv> {
        self.chain(|sb| sb.map_result(f))
    }

    /// Catch panics in execution and pipe them as errors
    #[cfg(feature = "catch-panic")]
    #[cfg_attr(docsrs, doc(cfg(feature = "catch-panic")))]
    fn catch_panic(
        self,
    ) -> WorkerBuilder<
        Req,
        Ctx,
        (),
        Stack<
            CatchPanicLayer<fn(Box<dyn std::any::Any + Send>) -> apalis_core::error::Error>,
            Middleware,
        >,
        Serv,
    > {
        self.chain(|svc| svc.layer(CatchPanicLayer::new()))
    }

    /// Enable tracing via tracing crate
    #[cfg(feature = "tracing")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tracing")))]
    fn enable_tracing(
        self,
    ) -> WorkerBuilder<Req, Ctx, (), Stack<tracing::TraceLayer, Middleware>, Serv> {
        use tracing::TraceLayer;

        self.chain(|svc| svc.layer(TraceLayer::new()))
    }
}
