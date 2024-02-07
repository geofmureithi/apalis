use apalis_core::error::Error;

use super::{LatencyUnit, DEFAULT_ERROR_LEVEL};

use std::time::Duration;
use tracing::{Level, Span};

/// Trait used to tell [`Trace`] what to do when a request fails.
///
/// See the [module docs](../trace/index.html#on_failure) for details on exactly when the
/// `on_failure` callback is called.
///
/// [`Trace`]: super::Trace
pub trait OnFailure {
    /// Do the thing.
    ///
    /// `latency` is the duration since the request was received.
    ///
    /// `span` is the `tracing` [`Span`], corresponding to this request, produced by the closure
    /// passed to [`TraceLayer::make_span_with`]. It can be used to [record field values][record]
    /// that weren't known when the span was created.
    ///
    /// [`Span`]: https://docs.rs/tracing/latest/tracing/span/index.html
    /// [record]: https://docs.rs/tracing/latest/tracing/span/struct.Span.html#method.record
    /// [`TraceLayer::make_span_with`]: crate::layers::tracing::TraceLayer::make_span_with
    fn on_failure(&mut self, error: &Error, latency: Duration, span: &Span);
}

impl OnFailure for () {
    #[inline]
    fn on_failure(&mut self, _: &Error, _: Duration, _: &Span) {}
}

impl<F> OnFailure for F
where
    F: FnMut(&Error, Duration, &Span),
{
    fn on_failure(&mut self, error: &Error, latency: Duration, span: &Span) {
        self(error, latency, span)
    }
}

/// The default [`OnFailure`] implementation used by [`Trace`].
///
/// [`Trace`]: super::Trace
#[derive(Clone, Debug)]
pub struct DefaultOnFailure {
    level: Level,
    latency_unit: LatencyUnit,
}

impl Default for DefaultOnFailure {
    fn default() -> Self {
        Self {
            level: DEFAULT_ERROR_LEVEL,
            latency_unit: LatencyUnit::Millis,
        }
    }
}

impl DefaultOnFailure {
    /// Create a new `DefaultOnFailure`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the [`Level`] used for [tracing events].
    ///
    /// Defaults to [`Level::ERROR`].
    ///
    /// [tracing events]: https://docs.rs/tracing/latest/tracing/#events
    pub fn level(mut self, level: Level) -> Self {
        self.level = level;
        self
    }

    /// Set the [`LatencyUnit`] latencies will be reported in.
    ///
    /// Defaults to [`LatencyUnit::Millis`].
    pub fn latency_unit(mut self, latency_unit: LatencyUnit) -> Self {
        self.latency_unit = latency_unit;
        self
    }
}

// Repeating this pattern match for each case is tedious. So we do it with a quick and
// dirty macro.
//
// Tracing requires all these parts to be declared statically. You cannot easily build
// events dynamically.
macro_rules! log_pattern_match {
    (
        $this:expr, $span:expr, $error:expr, $latency:expr, [$($level:ident),*]
    ) => {
        match ($this.level, $this.latency_unit) {
            $(
                (Level::$level, LatencyUnit::Seconds) => {
                    tracing::event!(
                        parent: $span,
                        Level::$level,
                        done_in = format_args!("{} s", $latency.as_secs_f64()),
                        "{}",
                        format_args!("{}", $error)
                    );
                }
                (Level::$level, LatencyUnit::Millis) => {
                    tracing::event!(
                        parent: $span,
                        Level::$level,
                        done_in = format_args!("{} ms", $latency.as_millis()),
                        "{}",
                        format_args!("{}", $error)
                    );
                }
                (Level::$level, LatencyUnit::Micros) => {
                    tracing::event!(
                        parent: $span,
                        Level::$level,
                        done_in = format_args!("{} μs", $latency.as_micros()),
                        "{}",
                        format_args!("{}", $error)
                    );
                }
                (Level::$level, LatencyUnit::Nanos) => {
                    tracing::event!(
                        parent: $span,
                        Level::$level,
                        done_in = format_args!("{} ns", $latency.as_nanos()),
                        "{}",
                        format_args!("{}", $error)
                    );
                }
            )*
        }
    };
}

impl OnFailure for DefaultOnFailure {
    fn on_failure(&mut self, error: &Error, latency: Duration, span: &Span) {
        log_pattern_match!(
            self,
            span,
            error,
            latency,
            [ERROR, WARN, INFO, DEBUG, TRACE]
        );
    }
}
