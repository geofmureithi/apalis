#[cfg(feature = "extensions")]
#[cfg_attr(docsrs, doc(cfg(feature = "extensions")))]
pub mod extensions;
#[cfg(feature = "prometheus")]
#[cfg_attr(docsrs, doc(cfg(feature = "prometheus")))]
pub mod prometheus;
#[cfg(feature = "retry")]
#[cfg_attr(docsrs, doc(cfg(feature = "retry")))]
pub mod retry;
#[cfg(feature = "sentry")]
#[cfg_attr(docsrs, doc(cfg(feature = "sentry")))]
pub mod sentry;
#[cfg(feature = "trace")]
#[cfg_attr(docsrs, doc(cfg(feature = "trace")))]
pub mod tracing;

#[cfg(feature = "limit")]
#[cfg_attr(docsrs, doc(cfg(feature = "limit")))]
pub mod limit {
    pub use tower::limit::RateLimitLayer;
}

#[cfg(feature = "timeout")]
#[cfg_attr(docsrs, doc(cfg(feature = "timeout")))]
pub use tower::timeout::TimeoutLayer;
