use std::pin::Pin;

use futures_core::Stream;
use futures_util::StreamExt;

use crate::backend::poll_strategy::{PollContext, PollStrategy};

mod stream;
pub use stream::*;
mod interval;
pub use interval::*;
mod future;
pub use future::*;
mod backoff;
pub use backoff::*;

/// A polling strategy that wraps another strategy
/// This is useful for coercing strategies
#[derive(Debug, Clone)]
pub struct WrapperStrategy<S>
where
    S: PollStrategy + Send + 'static,
{
    strategy: S,
}

impl<S> WrapperStrategy<S>
where
    S: PollStrategy + Send + 'static,
{
    /// Create a new WrapperStrategy from a strategy
    pub fn new(strategy: S) -> Self {
        Self { strategy }
    }
}

impl<S> PollStrategy for WrapperStrategy<S>
where
    S: PollStrategy + Send + 'static,
{
    type Stream = Pin<Box<dyn Stream<Item = ()> + Send>>;

    fn poll_strategy(self: Box<Self>, ctx: &PollContext) -> Self::Stream {
        Box::new(self.strategy)
            .poll_strategy(ctx)
            .map(|_| ())
            .boxed()
    }
}
