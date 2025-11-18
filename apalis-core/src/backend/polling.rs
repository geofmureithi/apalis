use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use std::{pin::Pin, sync::atomic::AtomicU64};

use futures_core::Stream;
use futures_core::stream::BoxStream;

use futures_util::future::poll_fn;
use futures_util::lock::Mutex;
use futures_util::{FutureExt, StreamExt, stream};

use crate::worker::context::WorkerContext;

/// A trait for different polling strategies
/// All strategies can be combined in a race condition
pub trait PollStrategy {
    /// The stream returned by the strategy
    type Stream: Stream<Item = ()> + Send;

    /// Create a stream that completes when the next poll should occur
    fn poll_ready(
        self: Box<Self>,
        ctx: &WorkerContext,
        prev_count: &Arc<AtomicUsize>,
    ) -> Self::Stream;
}

type BoxedPollStrategy =
    Box<dyn PollStrategy<Stream = Pin<Box<dyn Stream<Item = ()> + Send>>> + Send + Sync + 'static>;

struct WrapperStrategy<S>
where
    S: PollStrategy + Send + 'static,
{
    strategy: S,
}

impl<S> PollStrategy for WrapperStrategy<S>
where
    S: PollStrategy + Send + 'static,
{
    type Stream = Pin<Box<dyn Stream<Item = ()> + Send>>;

    fn poll_ready(
        self: Box<Self>,
        ctx: &WorkerContext,
        prev_count: &Arc<AtomicUsize>,
    ) -> Self::Stream {
        Box::new(self.strategy).poll_ready(ctx, prev_count).boxed()
    }
}

pub struct StrategyBuilder {
    strategies: Vec<BoxedPollStrategy>,
}

impl StrategyBuilder {
    pub fn new() -> Self {
        Self {
            strategies: Vec::new(),
        }
    }

    pub fn apply<S, Stm>(mut self, strategy: S) -> Self
    where
        S: PollStrategy<Stream = Stm> + 'static + Sync + Send,
        Stm: Stream<Item = ()> + Send + 'static,
    {
        self.strategies.push(Box::new(WrapperStrategy { strategy }));
        self
    }

    pub fn build(self) -> Strategy {
        Strategy {
            strategies: Arc::new(std::sync::Mutex::new(
                self.strategies
                    .into_iter()
                    .map(|s| Some(s))
                    .collect::<Vec<_>>(),
            )),
        }
    }
}

#[derive(Clone)]
pub struct Strategy {
    strategies: Arc<std::sync::Mutex<Vec<Option<BoxedPollStrategy>>>>,
}

impl Strategy {
    pub fn build(
        self,
        ctx: &WorkerContext,
        prev_count: &Arc<AtomicUsize>,
    ) -> Pin<Box<dyn Stream<Item = ()> + Send>> {
        let this = Box::new(self);
        this.poll_ready(ctx, prev_count)
    }
}

impl PollStrategy for Strategy {
    type Stream = Pin<Box<dyn Stream<Item = ()> + Send>>;

    fn poll_ready(
        mut self: Box<Self>,
        ctx: &WorkerContext,
        prev_count: &Arc<AtomicUsize>,
    ) -> Self::Stream {
        let ctx = ctx.clone();
        let prev_count = prev_count.clone();
        let ctx = ctx.clone();
        let mut streams = self
            .strategies
            .lock()
            .unwrap()
            .drain(..)
            .enumerate()
            .map(move |(i, mut a)| {
                let ctx = ctx.clone();
                let prev_count = prev_count.clone();
                let lock = a.take().expect("Strategy already taken");
                lock.poll_ready(&ctx, &prev_count)
            })
            .collect::<Vec<_>>();
        // Reverse to give priority to strategies in the order they were added
        streams.reverse();
        RaceNext::new(streams).map(|(_idx, _)| ()).boxed()
    }
}

/// A stream that polls multiple streams, always returning the first ready item,
/// and skipping one item from all other streams each round.
pub struct RaceNext<T> {
    streams: Vec<Option<Pin<Box<dyn Stream<Item = T> + Send>>>>,
    pending_skips: Vec<bool>,
}

impl<T: 'static + Send> RaceNext<T> {
    pub fn new(streams: Vec<Pin<Box<dyn Stream<Item = T> + Send>>>) -> Self {
        let len = streams.len();
        Self {
            streams: streams.into_iter().map(Some).collect(),
            pending_skips: vec![false; len],
        }
    }
}

impl<T: 'static + Send> Stream for RaceNext<T> {
    type Item = (usize, T);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        // First, handle any pending skips from the previous round
        for i in 0..this.streams.len() {
            if this.pending_skips[i] {
                if let Some(ref mut stream) = this.streams[i] {
                    match stream.as_mut().poll_next(cx) {
                        Poll::Ready(Some(_)) => {
                            // Successfully skipped an item
                            this.pending_skips[i] = false;
                        }
                        Poll::Ready(None) => {
                            // Stream ended while trying to skip
                            this.streams[i] = None;
                            this.pending_skips[i] = false;
                        }
                        Poll::Pending => {
                            // Still waiting to skip, continue to next stream
                            continue;
                        }
                    }
                }
            }
        }

        // Now poll for the next ready item
        let mut any_pending = false;
        for i in 0..this.streams.len() {
            // Skip streams that are still pending a skip operation
            if this.pending_skips[i] {
                any_pending = true;
                continue;
            }

            if let Some(ref mut stream) = this.streams[i] {
                match stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        // Found a ready item! Mark other streams for skipping
                        for j in 0..this.streams.len() {
                            if j != i && this.streams[j].is_some() {
                                this.pending_skips[j] = true;
                            }
                        }
                        return Poll::Ready(Some((i, item)));
                    }
                    Poll::Ready(None) => {
                        // This stream ended, remove it
                        this.streams[i] = None;
                    }
                    Poll::Pending => {
                        any_pending = true;
                    }
                }
            }
        }

        // Check if all streams are exhausted
        if this.streams.iter().all(|s| s.is_none()) {
            return Poll::Ready(None);
        }

        if any_pending {
            Poll::Pending
        } else {
            // All remaining streams are exhausted
            Poll::Ready(None)
        }
    }
}

impl<T: 'static + Send> RaceNext<T> {
    /// Returns the number of active streams remaining
    pub fn active_count(&self) -> usize {
        self.streams.iter().filter(|s| s.is_some()).count()
    }

    /// Checks if any streams are still active
    pub fn has_active_streams(&self) -> bool {
        self.streams.iter().any(|s| s.is_some())
    }
}

// Simple PRNG state for jitter (thread-safe)
static JITTER_STATE: AtomicU64 = AtomicU64::new(1);

/// Backoff configuration for strategies
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    pub max_delay: Duration,
    pub multiplier: f64,
    pub jitter_factor: f64, // 0.0 to 1.0
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
    pub fn new(max: Duration) -> Self {
        Self {
            max_delay: max,
            ..Default::default()
        }
    }

    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier;
        self
    }

    pub fn with_jitter(mut self, jitter_factor: f64) -> Self {
        self.jitter_factor = jitter_factor.clamp(0.0, 1.0);
        self
    }

    /// Calculate the next delay with backoff and jitter
    pub fn next_delay(
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

/// Interval-based polling strategy with optional backoff
#[derive(Clone)]
pub struct IntervalStrategy {
    interval: Duration,
    current_delay: Duration,
}

pub struct BackoffStrategy {
    interval: IntervalStrategy,
    backoff_config: BackoffConfig,
    default_delay: Duration,
}

impl IntervalStrategy {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            current_delay: interval,
        }
    }

    pub fn with_backoff(self, config: BackoffConfig) -> BackoffStrategy {
        BackoffStrategy {
            default_delay: self.interval,
            interval: self,
            backoff_config: config,
        }
    }
}

impl PollStrategy for IntervalStrategy {
    type Stream = BoxStream<'static, ()>;

    fn poll_ready(self: Box<Self>, ctx: &WorkerContext, prev: &Arc<AtomicUsize>) -> Self::Stream {
        let interval = self.interval;
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

impl PollStrategy for BackoffStrategy {
    type Stream = BoxStream<'static, ()>;

    fn poll_ready(self: Box<Self>, _ctx: &WorkerContext, prev: &Arc<AtomicUsize>) -> Self::Stream {
        let backoff_config = self.backoff_config.clone();
        let current_delay = self.interval.interval.clone();
        let default_delay = self.default_delay;

        stream::unfold(
            (prev.clone(), current_delay),
            move |(prev, mut current_delay)| {
                let fut = futures_timer::Delay::new(current_delay);
                let backoff_config = backoff_config.clone();
                async move {
                    fut.await;
                    let failed = prev.load(Ordering::Relaxed) == 0;
                    current_delay = backoff_config.next_delay(default_delay, current_delay, failed);
                    Some(((), (prev, current_delay)))
                }
            },
        )
        .boxed()
    }
}

#[derive(Clone)]
pub struct FutureStrategy<F> {
    future_factory: Arc<Mutex<dyn FnMut(WorkerContext, usize) -> F + Send>>,
}

impl<F> FutureStrategy<F>
where
    F: Future<Output = ()> + Send + 'static,
{
    pub fn new<Factory>(factory: Factory) -> Self
    where
        Factory: FnMut(WorkerContext, usize) -> F + Send + 'static,
    {
        Self {
            future_factory: Arc::new(Mutex::new(Box::new(factory))),
        }
    }
}

impl<F> PollStrategy for FutureStrategy<F>
where
    F: Future<Output = ()> + Send + 'static,
{
    type Stream = Pin<Box<dyn Stream<Item = ()> + Send>>;
    fn poll_ready(self: Box<Self>, ctx: &WorkerContext, prev: &Arc<AtomicUsize>) -> Self::Stream {
        let factory = self.future_factory;
        let ctx = ctx.clone();

        stream::unfold((ctx, prev.clone()), move |(ctx, prev)| {
            let factory = factory.clone();
            async move {
                let fut = {
                    let mut lock = factory.try_lock().unwrap();
                    (lock)(ctx.clone(), prev.load(Ordering::Relaxed))
                };
                fut.await;
                Some(((), (ctx, prev)))
            }
        })
        .boxed()
    }
}
