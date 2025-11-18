use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};

use crate::backend::poll_strategy::{IntervalStrategy, PollContext, PollStrategy};

// Simple PRNG state for jitter (thread-safe)
static JITTER_STATE: AtomicU64 = AtomicU64::new(1);

/// A polling strategy that applies exponential backoff to an inner interval strategy
#[derive(Clone, Debug)]
pub struct BackoffStrategy {
    interval: IntervalStrategy,
    backoff_config: BackoffConfig,
    default_delay: Duration,
}
impl BackoffStrategy {
    /// Create a new BackoffStrategy wrapping an IntervalStrategy with the given BackoffConfig
    #[must_use]
    pub fn new(inner: IntervalStrategy, config: BackoffConfig) -> Self {
        Self {
            default_delay: inner.poll_interval,
            interval: inner,
            backoff_config: config,
        }
    }
}

impl PollStrategy for BackoffStrategy {
    type Stream = BoxStream<'static, ()>;

    fn poll_strategy(self: Box<Self>, ctx: &PollContext) -> Self::Stream {
        let backoff_config = self.backoff_config.clone();
        let current_delay = self.interval.poll_interval;
        let default_delay = self.default_delay;

        stream::unfold(
            (ctx.clone(), current_delay),
            move |(ctx, mut current_delay)| {
                let fut = futures_timer::Delay::new(current_delay);
                let backoff_config = backoff_config.clone();
                async move {
                    fut.await;
                    let failed = ctx.prev_count.load(Ordering::Relaxed) == 0;
                    current_delay = backoff_config.next_delay(default_delay, current_delay, failed);
                    Some(((), (ctx, current_delay)))
                }
            },
        )
        .boxed()
    }
}

/// Backoff configuration for strategies
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    max_delay: Duration,
    multiplier: f64,
    jitter_factor: f64, // 0.0 to 1.0
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
            jitter_factor: 0.1,
        }
    }
}

impl BackoffConfig {
    /// Create a new BackoffConfig with the specified maximum delay
    #[must_use]
    pub fn new(max: Duration) -> Self {
        Self {
            max_delay: max,
            ..Default::default()
        }
    }

    /// Set the multiplier for exponential backoff
    #[must_use]
    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier;
        self
    }

    /// Set the jitter factor (0.0 to 1.0) for randomizing delays
    #[must_use]
    pub fn with_jitter(mut self, jitter_factor: f64) -> Self {
        self.jitter_factor = jitter_factor.clamp(0.0, 1.0);
        self
    }

    /// Calculate the next delay with backoff and jitter
    fn next_delay(
        &self,
        default_delay: Duration,
        current_delay: Duration,
        failed: bool,
    ) -> Duration {
        let base_delay = if failed {
            // Exponential backoff on failure
            let next = Duration::from_secs_f64(current_delay.as_secs_f64() * self.multiplier);
            next.min(self.max_delay)
        } else {
            // Reset to initial on success
            default_delay
        };

        // Add jitter using a simple LCG (Linear Congruential Generator)
        if self.jitter_factor > 0.0 {
            // Simple deterministic pseudo-random number generation
            let mut state = JITTER_STATE.load(Ordering::Relaxed);
            state = state.wrapping_mul(1103515245).wrapping_add(12345);
            JITTER_STATE.store(state, Ordering::Relaxed);

            // Convert to 0.0-1.0 range
            let normalized = (state as f64) / (u64::MAX as f64);

            // Apply jitter: -jitter_factor to +jitter_factor
            let jitter_range = base_delay.as_secs_f64() * self.jitter_factor;
            let jitter = (normalized - 0.5) * 2.0 * jitter_range;
            let jittered = base_delay.as_secs_f64() + jitter;
            Duration::from_secs_f64(jittered.max(0.0))
        } else {
            base_delay
        }
    }
}
