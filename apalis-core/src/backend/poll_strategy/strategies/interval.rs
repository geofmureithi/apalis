use std::time::Duration;

use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};

use crate::backend::poll_strategy::{BackoffConfig, BackoffStrategy, PollContext, PollStrategy};

/// Interval-based polling strategy with optional backoff
#[derive(Debug, Clone)]
pub struct IntervalStrategy {
    pub(super) poll_interval: Duration,
}

impl IntervalStrategy {
    /// Create a new IntervalStrategy with the specified interval
    pub fn new(interval: Duration) -> Self {
        Self {
            poll_interval: interval,
        }
    }

    /// Wrap the IntervalStrategy with a BackoffStrategy
    /// This will apply exponential backoff to the polling interval
    /// based on the provided [`BackoffConfig`].`
    pub fn with_backoff(self, config: BackoffConfig) -> BackoffStrategy {
        BackoffStrategy::new(self, config)
    }
}

impl PollStrategy for IntervalStrategy {
    type Stream = BoxStream<'static, ()>;

    fn poll_strategy(self: Box<Self>, _: &PollContext) -> Self::Stream {
        let interval = self.poll_interval;
        stream::unfold((), move |()| {
            let fut = futures_timer::Delay::new(interval);
            async move {
                fut.await;
                Some(((), ()))
            }
        })
        .boxed()
    }
}
