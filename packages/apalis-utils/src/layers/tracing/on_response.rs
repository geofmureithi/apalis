use super::LatencyUnit;
use super::DEFAULT_MESSAGE_LEVEL;
use std::fmt::Debug;
use std::time::Duration;
use tracing::Level;
use tracing::Span;

/// Trait used to tell [`Trace`] what to do when a response has been produced.
///
/// See the [module docs](../trace/index.html#on_response) for details on exactly when the
/// `on_response` callback is called.
///
/// [`Trace`]: super::Trace
pub trait OnResponse<Res> {
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
    fn on_response(self, res: &Res, done_in: Duration, span: &Span);
}

impl<Res> OnResponse<Res> for () {
    #[inline]
    fn on_response(self, _: &Res, _: Duration, _: &Span) {}
}

impl<F, Res> OnResponse<Res> for F
where
    F: FnOnce(&Res, Duration, &Span),
{
    fn on_response(self, response: &Res, done_in: Duration, span: &Span) {
        self(response, done_in, span)
    }
}

/// The default [`OnResponse`] implementation used by [`Trace`].
///
/// [`Trace`]: super::Trace
#[derive(Clone, Debug)]
pub struct DefaultOnResponse {
    level: Level,
    latency_unit: LatencyUnit,
}

impl Default for DefaultOnResponse {
    fn default() -> Self {
        Self {
            level: DEFAULT_MESSAGE_LEVEL,
            latency_unit: LatencyUnit::Millis,
        }
    }
}

impl DefaultOnResponse {
    /// Create a new `DefaultOnResponse`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the [`Level`] used for [tracing events].
    ///
    /// Please note that while this will set the level for the tracing events
    /// themselves, it might cause them to lack expected information, like
    /// request method or path. You can address this using
    /// [`DefaultMakeSpan::level`].
    ///
    /// Defaults to [`Level::DEBUG`].
    ///
    /// [tracing events]: https://docs.rs/tracing/latest/tracing/#events
    /// [`DefaultMakeSpan::level`]: crate::layers::tracing::DefaultMakeSpan::level
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
#[allow(unused_macros)]
macro_rules! log_pattern_match {
    (
        $this:expr,  $res:expr, $done_in:expr, [$($level:ident),*]
    ) => {
        match ($this.level, $this.latency_unit) {
            $(
                (Level::$level, LatencyUnit::Seconds) => {
                    tracing::event!(
                        Level::$level,
                        done_in = format_args!("{}s", $done_in.as_secs_f64()),
                        result = format_args!("{:?}", $res),
                        "job.done"
                    );
                }
                (Level::$level, LatencyUnit::Millis) => {
                    tracing::event!(
                        Level::$level,
                        done_in = format_args!("{}ms", $done_in.as_millis()),
                        result = format_args!("{:?}", $res),
                        "job.done"
                    );
                }
                (Level::$level, LatencyUnit::Micros) => {
                    tracing::event!(
                        Level::$level,
                        done_in = format_args!("{}μs", $done_in.as_micros()),
                        result = format_args!("{:?}", $res),
                        "job.done"
                    );
                }
                (Level::$level, LatencyUnit::Nanos) => {
                    tracing::event!(
                        Level::$level,
                        done_in = format_args!("{}ns", $done_in.as_nanos()),
                        result = format_args!("{:?}", $res),
                        "job.done"
                    );
                }

            )*
        }
    };
}

impl<Res: Debug> OnResponse<Res> for DefaultOnResponse {
    fn on_response(self, response: &Res, done_in: Duration, _: &Span) {
        log_pattern_match!(self, response, done_in, [ERROR, WARN, INFO, DEBUG, TRACE]);
    }
}
