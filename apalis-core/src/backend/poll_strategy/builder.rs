use futures_core::Stream;
use futures_util::StreamExt;
use std::{pin::Pin, sync::Arc};

use crate::backend::poll_strategy::{
    BoxedPollStrategy, PollContext, PollStrategy, RaceNext, WrapperStrategy,
};

/// Builder for composing multiple polling strategies
pub struct StrategyBuilder {
    strategies: Vec<BoxedPollStrategy>,
}

impl std::fmt::Debug for StrategyBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StrategyBuilder")
            .field("strategies", &self.strategies.len())
            .finish()
    }
}

impl Default for StrategyBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl StrategyBuilder {
    /// Create a new StrategyBuilder
    #[must_use]
    pub fn new() -> Self {
        Self {
            strategies: Vec::new(),
        }
    }

    /// Apply a polling strategy to the builder
    /// Strategies are executed in the order they are added, with the first strategy having the highest priority
    /// In case of multiple strategies being ready at the same time, the first one added will be chosen
    #[must_use]
    pub fn apply<S, Stm>(mut self, strategy: S) -> Self
    where
        S: PollStrategy<Stream = Stm> + 'static + Sync + Send,
        Stm: Stream<Item = ()> + Send + 'static,
    {
        self.strategies
            .push(Box::new(WrapperStrategy::new(strategy)));
        self
    }

    /// Build the MultiStrategy from the builder
    /// Consumes the builder and returns a MultiStrategy
    /// The MultiStrategy will contain all the strategies added to the builder
    #[must_use]
    pub fn build(self) -> MultiStrategy {
        MultiStrategy {
            strategies: Arc::new(std::sync::Mutex::new(self.strategies)),
        }
    }
}

/// A polling strategy that combines multiple strategies
/// The strategies are executed in the order they were added to the builder
/// In case of multiple strategies being ready at the same time, the first one added will be chosen
#[derive(Clone)]
pub struct MultiStrategy {
    strategies: Arc<std::sync::Mutex<Vec<BoxedPollStrategy>>>,
}

impl std::fmt::Debug for MultiStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiStrategy")
            .field("strategies", &self.strategies.lock().unwrap().len())
            .finish()
    }
}

impl PollStrategy for MultiStrategy {
    type Stream = Pin<Box<dyn Stream<Item = ()> + Send>>;

    fn poll_strategy(self: Box<Self>, ctx: &PollContext) -> Self::Stream {
        let ctx = ctx.clone();
        let mut streams = self
            .strategies
            .lock()
            .unwrap()
            .drain(..)
            .map(move |s| {
                let ctx = ctx.clone();
                s.poll_strategy(&ctx)
            })
            .collect::<Vec<_>>();
        // Reverse to give priority to strategies in the order they were added
        streams.reverse();
        RaceNext::new(streams).map(|(_idx, _)| ()).boxed()
    }
}
