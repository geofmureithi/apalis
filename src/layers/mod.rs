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
    pub use tower::limit::RateLimitLayer;
}

/// Timeout middleware for apalis
#[cfg(feature = "timeout")]
#[cfg_attr(docsrs, doc(cfg(feature = "timeout")))]
pub use tower::timeout::TimeoutLayer;
