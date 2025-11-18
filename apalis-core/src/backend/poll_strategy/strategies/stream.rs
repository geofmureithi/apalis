use futures_core::Stream;

use crate::backend::poll_strategy::{PollContext, PollStrategy};

/// A polling strategy that uses a provided stream
#[derive(Debug, Clone)]
pub struct StreamStrategy<S> {
    stm: S,
}

impl<S> StreamStrategy<S>
where
    S: Stream<Item = ()> + Unpin + Send + 'static,
{
    /// Create a new StreamStrategy from a stream
    pub fn new(stm: S) -> Self {
        Self { stm }
    }
}

impl<S> PollStrategy for StreamStrategy<S>
where
    S: Stream<Item = ()> + Unpin + Send + 'static,
{
    type Stream = S;

    fn poll_strategy(self: Box<Self>, _ctx: &PollContext) -> Self::Stream {
        self.stm
    }
}
