use apalis_core::request::Request;
use tracing::{Level, Span};

use super::DEFAULT_MESSAGE_LEVEL;

/// Trait used to generate [`Span`]s from requests. [`Trace`] wraps all request handling in this
/// span.
///
/// [`Span`]: tracing::Span
/// [`Trace`]: super::Trace
pub trait MakeSpan<B, Ctx> {
    /// Make a span from a request.
    fn make_span(&mut self, request: &Request<B, Ctx>) -> Span;
}

impl<B, Ctx> MakeSpan<B, Ctx> for Span {
    fn make_span(&mut self, _request: &Request<B, Ctx>) -> Span {
        self.clone()
    }
}

impl<F, B, Ctx> MakeSpan<B, Ctx> for F
where
    F: FnMut(&Request<B, Ctx>) -> Span,
{
    fn make_span(&mut self, request: &Request<B, Ctx>) -> Span {
        self(request)
    }
}

/// The default way [`Span`]s will be created for [`Trace`].
///
/// [`Span`]: tracing::Span
/// [`Trace`]: super::Trace
#[derive(Debug, Clone)]
pub struct DefaultMakeSpan {
    level: Level,
}

impl DefaultMakeSpan {
    /// Create a new `DefaultMakeSpan`.
    pub fn new() -> Self {
        Self {
            level: DEFAULT_MESSAGE_LEVEL,
        }
    }

    /// Set the [`Level`] used for the [tracing span].
    ///
    /// Defaults to [`Level::DEBUG`].
    ///
    /// [tracing span]: https://docs.rs/tracing/latest/tracing/#spans
    pub fn level(mut self, level: Level) -> Self {
        self.level = level;
        self
    }
}

impl Default for DefaultMakeSpan {
    fn default() -> Self {
        Self::new()
    }
}

impl<B, Ctx> MakeSpan<B, Ctx> for DefaultMakeSpan {
    fn make_span(&mut self, req: &Request<B, Ctx>) -> Span {
        // This ugly macro is needed, unfortunately, because `tracing::span!`
        // required the level argument to be static. Meaning we can't just pass
        // `self.level`.
        let task_id = req.parts.task_id.to_string();
        let attempt = &req.parts.attempt;
        let span = Span::current();
        macro_rules! make_span {
            ($level:expr) => {
                tracing::span!(
                    parent: span,
                    $level,
                    "task",
                    task_id = task_id,
                    attempt = attempt.current()
                )
            };
        }

        match self.level {
            Level::ERROR => {
                make_span!(Level::ERROR)
            }
            Level::WARN => {
                make_span!(Level::WARN)
            }
            Level::INFO => {
                make_span!(Level::INFO)
            }
            Level::DEBUG => {
                make_span!(Level::DEBUG)
            }
            Level::TRACE => {
                make_span!(Level::TRACE)
            }
        }
    }
}
