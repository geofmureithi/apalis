//! Polling strategies for backends.
//!
//! This module provides abstractions and implementations for different polling strategies
//! used by backends to determine when to poll for new tasks. Strategies can be
//! combined, customized, and extended to suit various workload requirements.
//!
//! # Features
//!
//! - Trait [`PollStrategy`] for defining custom polling strategies.
//! - Extension trait [`PollStrategyExt`] for ergonomic usage.
//! - [`PollContext`] struct for passing contextual information to strategies.
//! - Boxed trait object type [`BoxedPollStrategy`] for dynamic dispatch.
//! - Built-in strategies and combinators.
//!
//! # Usage
//!
//! Implement the [`PollStrategy`] trait for your custom strategy, or use the provided
//! strategies and combinators. Use [`PollContext`] to access worker state and previous
//! task counts.
//!
//! See submodules for available strategies and builder utilities.
use crate::worker::context::WorkerContext;
use futures_core::Stream;
use futures_util::StreamExt;
use std::{
    pin::Pin,
    sync::{Arc, atomic::AtomicUsize},
};

mod strategies;
pub use strategies::*;
mod builder;
pub use builder::*;
mod race_next;
pub use race_next::*;

/// A boxed poll strategy
pub type BoxedPollStrategy =
    Box<dyn PollStrategy<Stream = Pin<Box<dyn Stream<Item = ()> + Send>>> + Send + Sync + 'static>;

/// A trait for different polling strategies
/// All strategies can be combined in a race condition
pub trait PollStrategy {
    /// The stream returned by the strategy
    type Stream: Stream + Send;

    /// Create a stream that completes when the next poll should occur
    fn poll_strategy(self: Box<Self>, ctx: &PollContext) -> Self::Stream;
}

impl<T: Sized> PollStrategyExt for T where T: PollStrategy {}

/// Extension trait for PollStrategy
pub trait PollStrategyExt: PollStrategy + Sized {
    /// Build a boxed stream from the strategy
    /// This is a convenience method that boxes the strategy and calls `poll_strategy`
    fn build_stream(self, ctx: &PollContext) -> Pin<Box<dyn Stream<Item = ()> + Send>>
    where
        Self::Stream: 'static,
    {
        let this = Box::new(self);
        this.poll_strategy(ctx).map(|_| ()).boxed()
    }
}

/// Context provided to the polling strategies
/// Includes the worker context and a reference to the previous count of tasks received
#[derive(Debug, Clone)]
pub struct PollContext {
    worker: WorkerContext,
    prev_count: Arc<AtomicUsize>,
}

impl PollContext {
    /// Create a new PollContext
    pub fn new(worker: WorkerContext, prev_count: Arc<AtomicUsize>) -> Self {
        Self { worker, prev_count }
    }
    /// Get a reference to the worker context
    #[must_use]
    pub fn worker(&self) -> &WorkerContext {
        &self.worker
    }
    /// Get a reference to the previous count of tasks received
    #[must_use]
    pub fn prev_count(&self) -> &Arc<AtomicUsize> {
        &self.prev_count
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, atomic::Ordering},
        time::Duration,
    };

    use futures_channel::mpsc;

    use futures_util::{
        FutureExt, SinkExt, StreamExt,
        lock::Mutex,
        sink,
        stream::{self},
    };

    use crate::{
        error::BoxDynError,
        task::Task,
        worker::{
            builder::WorkerBuilder, context::WorkerContext, ext::event_listener::EventListenerExt,
        },
    };

    use super::*;

    const ITEMS: u32 = 10;

    type InMemoryQueue<T> = Arc<Mutex<VecDeque<Task<T, ()>>>>;

    #[tokio::test]
    #[cfg(feature = "sleep")]
    async fn basic_strategy_backend() {
        use crate::backend::custom::BackendBuilder;

        let memory: InMemoryQueue<u32> = Arc::new(Mutex::new(VecDeque::new()));

        #[derive(Clone)]
        struct Config {
            strategy: MultiStrategy,
            prev_count: Arc<AtomicUsize>,
        }
        let strategy = StrategyBuilder::new()
            .apply(IntervalStrategy::new(Duration::from_millis(100)))
            .build();

        let config = Config {
            strategy,
            prev_count: Arc::new(AtomicUsize::new(1)),
        };

        let mut backend = BackendBuilder::new_with_cfg(config)
            .database(memory)
            .fetcher(|db, config, worker| {
                let poll_strategy = config.strategy.clone();
                let poll_ctx = PollContext::new(worker.clone(), config.prev_count.clone());
                let poller = poll_strategy.build_stream(&poll_ctx);
                stream::unfold(
                    (db.clone(), config.clone(), poller, worker.clone()),
                    |(p, config, mut poller, ctx)| async move {
                        let _ = poller.next().await;
                        let mut db = p.lock().await;
                        let item = db.pop_front();
                        drop(db);
                        if let Some(item) = item {
                            config.prev_count.store(1, Ordering::Relaxed);
                            Some((Ok::<_, BoxDynError>(Some(item)), (p, config, poller, ctx)))
                        } else {
                            config.prev_count.store(0, Ordering::Relaxed);
                            Some((
                                Ok::<Option<Task<u32, ()>>, BoxDynError>(None),
                                (p, config, poller, ctx),
                            ))
                        }
                    },
                )
                .boxed()
            })
            .sink(|db, _| {
                sink::unfold(db.clone(), move |p, item| {
                    async move {
                        let mut db = p.lock().await;
                        db.push_back(item);
                        drop(db);
                        Ok::<_, BoxDynError>(p)
                    }
                    .boxed()
                })
            })
            .build()
            .unwrap();

        for i in 0..ITEMS {
            backend.send(Task::new(i)).await.unwrap();
        }

        async fn task(task: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task == ITEMS - 1 {
                tokio::time::sleep(Duration::from_secs(5)).await;
                ctx.stop().unwrap();
                return Err("Worker stopped!")?;
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                println!("On Event = {ev:?} from {}", ctx.name());
            })
            .build(task);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "sleep")]
    async fn custom_strategy_backend() {
        use crate::backend::custom::BackendBuilder;

        let memory: InMemoryQueue<u32> = Arc::new(Mutex::new(VecDeque::new()));

        #[derive(Clone)]
        struct Config {
            strategy: MultiStrategy,
            prev_count: Arc<AtomicUsize>,
        }

        let backoff = BackoffConfig::new(Duration::from_secs(5))
            .with_multiplier(1.5)
            .with_jitter(0.2);
        let interval = IntervalStrategy::new(Duration::from_millis(200)).with_backoff(backoff);

        let when_i_am_ready = FutureStrategy::new(|_ctx: WorkerContext, _prev: usize| {
            // println!("Waiting to be ready...");
            tokio::time::sleep(Duration::from_millis(1500)).map(|_| {
                // println!("I am ready now!");
            })
        });

        let (mut tx, rx) = mpsc::channel(1);

        tokio::spawn(async move {
            for i in 0..ITEMS {
                tokio::time::sleep(Duration::from_secs((i) as u64)).await;
                if tx.send(()).await.is_err() {
                    break;
                }
            }
        });

        let strategy = StrategyBuilder::new()
            .apply(when_i_am_ready)
            .apply(interval)
            .apply(StreamStrategy::new(rx))
            .build();

        let config = Config {
            strategy,
            prev_count: Arc::new(AtomicUsize::new(1)),
        };

        let mut backend = BackendBuilder::new_with_cfg(config)
            .database(memory)
            .fetcher(|db, config, worker| {
                let poll_strategy = config.strategy.clone();
                let poll_ctx = PollContext::new(worker.clone(), config.prev_count.clone());
                let poller = poll_strategy.build_stream(&poll_ctx);
                stream::unfold(
                    (db.clone(), config.clone(), poller, worker.clone()),
                    |(p, config, mut poller, ctx)| async move {
                        poller.next().await;
                        let mut db = p.lock().await;
                        let item = db.pop_front();
                        drop(db);
                        if let Some(item) = item {
                            config.prev_count.store(1, Ordering::Relaxed);
                            Some((Ok::<_, BoxDynError>(Some(item)), (p, config, poller, ctx)))
                        } else {
                            config.prev_count.store(0, Ordering::Relaxed);
                            Some((
                                Ok::<Option<Task<u32, ()>>, BoxDynError>(None),
                                (p, config, poller, ctx),
                            ))
                        }
                    },
                )
                .boxed()
            })
            .sink(|db, _| {
                sink::unfold(db.clone(), move |p, item| {
                    async move {
                        let mut db = p.lock().await;
                        db.push_back(item);
                        drop(db);
                        Ok::<_, BoxDynError>(p)
                    }
                    .boxed()
                })
            })
            .build()
            .unwrap();

        for i in 0..ITEMS {
            backend.send(Task::new(i)).await.unwrap();
        }

        async fn task(task: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task == ITEMS - 1 {
                tokio::time::sleep(Duration::from_secs(10)).await;
                ctx.stop().unwrap();
                return Err("Worker stopped!")?;
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                println!("On Event = {ev:?} from {}", ctx.name());
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
