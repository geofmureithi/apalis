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

#[cfg(feature = "catch-panic")]
use apalis_core::error::AbortError;
use apalis_core::{backend::Backend, worker::builder::WorkerBuilder};
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
pub trait WorkerBuilderExt<Args, Ctx, Source, Middleware> {
    /// Optionally adds a new layer `T` into the [`WorkerBuilder`].
    fn option_layer<T>(
        self,
        layer: Option<T>,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::util::Either<T, Identity>, Middleware>>;

    /// Adds a [`Layer`] built from a function that accepts a service and returns another service.
    fn layer_fn<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::layer::LayerFn<F>, Middleware>>;

    /// Limits the max number of in-flight requests.
    #[cfg(feature = "limit")]
    fn concurrency(
        self,
        max: usize,
    ) -> WorkerBuilder<Args, Source, Stack<tower::limit::ConcurrencyLimitLayer, Middleware>>;

    /// Limits requests to at most `num` per the given duration.
    #[cfg(feature = "limit")]
    fn rate_limit(
        self,
        num: u64,
        per: std::time::Duration,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::limit::RateLimitLayer, Middleware>>;

    /// Retries failed requests according to the given retry policy.
    #[cfg(feature = "retry")]
    fn retry<P>(
        self,
        policy: P,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::retry::RetryLayer<P>, Middleware>>;

    /// Fails requests that take longer than `timeout`.
    #[cfg(feature = "timeout")]
    fn timeout(
        self,
        timeout: std::time::Duration,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::timeout::TimeoutLayer, Middleware>>;

    /// Conditionally rejects requests based on `predicate`.
    #[cfg(feature = "filter")]
    fn filter<P>(
        self,
        predicate: P,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::filter::FilterLayer<P>, Middleware>>;

    /// Conditionally rejects requests based on an asynchronous `predicate`.
    #[cfg(feature = "filter")]
    fn filter_async<P>(
        self,
        predicate: P,
    ) -> WorkerBuilder<
        Args,
        Source,
        Srv,
        Stack<tower::filter::AsyncFilterLayer<P>, Middleware>,
        Worker,
    >;

    /// Maps one request type to another.
    fn map_request<F, R1, R2>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::util::MapRequestLayer<F>, Middleware>>
    where
        F: FnMut(R1) -> R2 + Clone;

    /// Maps one response type to another.
    fn map_response<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::util::MapResponseLayer<F>, Middleware>>;

    /// Maps one error type to another.
    fn map_err<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::util::MapErrLayer<F>, Middleware>>;

    /// Composes a function that transforms futures produced by the service.
    fn map_future<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::util::MapFutureLayer<F>, Middleware>>;

    /// Applies an asynchronous function after the service, regardless of whether the future succeeds or fails.
    fn then<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::util::ThenLayer<F>, Middleware>>;

    /// Executes a new future after this service's future resolves.
    fn and_then<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::util::AndThenLayer<F>, Middleware>>;

    /// Maps the service's result type to a different value, regardless of success or failure.
    fn map_result<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::util::MapResultLayer<F>, Middleware>>;

    /// Catch panics in execution and pipe them as errors
    #[cfg(feature = "catch-panic")]
    #[cfg_attr(docsrs, doc(cfg(feature = "catch-panic")))]
    #[allow(clippy::type_complexity)]
    fn catch_panic(
        self,
    ) -> WorkerBuilder<
        Args,
        Ctx,
        Source,
        Stack<
            CatchPanicLayer<fn(Box<dyn std::any::Any + Send>) -> AbortError, AbortError>,
            Middleware,
        >,
    >;
    /// Enable tracing via tracing crate
    #[cfg(feature = "tracing")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tracing")))]
    fn enable_tracing(
        self,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tracing::TraceLayer, Middleware>>;
}

impl<Args, Ctx, Source, Middleware> WorkerBuilderExt<Args, Ctx, Source, Middleware>
    for WorkerBuilder<Args, Ctx, Source, Middleware> where Source: Backend<Args>
{
    fn option_layer<T>(
        self,
        layer: Option<T>,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::util::Either<T, Identity>, Middleware>> {
        self.layer(tower::util::option_layer(layer))
    }

    fn layer_fn<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::layer::LayerFn<F>, Middleware>> {
        self.layer(tower::layer::layer_fn(f))
    }

    #[cfg(feature = "limit")]
    fn concurrency(
        self,
        max: usize,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::limit::ConcurrencyLimitLayer, Middleware>>
    {
        self.layer(tower::limit::ConcurrencyLimitLayer::new(max))
    }

    #[cfg(feature = "limit")]
    fn rate_limit(
        self,
        num: u64,
        per: std::time::Duration,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::limit::RateLimitLayer, Middleware>> {
        self.layer(tower::limit::RateLimitLayer::new(num, per))
    }

    #[cfg(feature = "retry")]
    fn retry<P>(
        self,
        policy: P,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::retry::RetryLayer<P>, Middleware>> {
        self.layer(tower::retry::RetryLayer::new(policy))
    }

    #[cfg(feature = "timeout")]
    fn timeout(
        self,
        timeout: std::time::Duration,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::timeout::TimeoutLayer, Middleware>> {
        self.layer(tower::timeout::TimeoutLayer::new(timeout))
    }

    #[cfg(feature = "filter")]
    fn filter<P>(
        self,
        predicate: P,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::filter::FilterLayer<P>, Middleware>> {
        self.layer(tower::filter::FilterLayer::new(predicate))
    }

    #[cfg(feature = "filter")]
    fn filter_async<P>(
        self,
        predicate: P,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::filter::AsyncFilterLayer<P>, Middleware>>
    {
        self.layer(tower::filter::AsyncFilterLayer::new(predicate))
    }

    fn map_request<F, R1, R2>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::util::MapRequestLayer<F>, Middleware>>
    where
        F: FnMut(R1) -> R2 + Clone,
    {
        self.layer(tower::util::MapRequestLayer::new(f))
    }

    fn map_response<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::util::MapResponseLayer<F>, Middleware>> {
        self.layer(tower::util::MapResponseLayer::new(f))
    }

    fn map_err<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::util::MapErrLayer<F>, Middleware>> {
        self.layer(tower::util::MapErrLayer::new(f))
    }

    fn map_future<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::util::MapFutureLayer<F>, Middleware>> {
        self.layer(tower::util::MapFutureLayer::new(f))
    }

    fn then<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::util::ThenLayer<F>, Middleware>> {
        self.layer(tower::util::ThenLayer::new(f))
    }

    fn and_then<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::util::AndThenLayer<F>, Middleware>> {
        self.layer(tower::util::AndThenLayer::new(f))
    }

    fn map_result<F>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tower::util::MapResultLayer<F>, Middleware>> {
        self.layer(tower::util::MapResultLayer::new(f))
    }

    /// Catch panics in execution and pipe them as errors
    #[cfg(feature = "catch-panic")]
    #[cfg_attr(docsrs, doc(cfg(feature = "catch-panic")))]
    fn catch_panic(
        self,
    ) -> WorkerBuilder<
        Args,
        Ctx,
        Source,
        Stack<
            CatchPanicLayer<fn(Box<dyn std::any::Any + Send>) -> AbortError, AbortError>,
            Middleware,
        >,
    > {
        self.layer(CatchPanicLayer::new())
    }

    /// Enable tracing via tracing crate
    #[cfg(feature = "tracing")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tracing")))]
    fn enable_tracing(
        self,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<tracing::TraceLayer, Middleware>> {
        use tracing::TraceLayer;

        self.layer(TraceLayer::new())
    }
}
