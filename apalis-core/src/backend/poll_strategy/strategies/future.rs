use core::fmt;
use std::{
    pin::Pin,
    sync::{Arc, atomic::Ordering},
};

use futures_core::Stream;
use futures_util::{StreamExt, lock::Mutex, stream};

use crate::{
    backend::poll_strategy::{PollContext, PollStrategy},
    worker::context::WorkerContext,
};

/// A polling strategy that uses a future factory to create futures for polling
#[derive(Clone)]
pub struct FutureStrategy<F> {
    future_factory: Arc<Mutex<dyn FnMut(WorkerContext, usize) -> F + Send>>,
}

impl<F> fmt::Debug for FutureStrategy<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FutureStrategy")
            .field("future_factory", &"FnMut(WorkerContext, usize) -> F")
            .finish()
    }
}

impl<F> FutureStrategy<F>
where
    F: Future<Output = ()> + Send + 'static,
{
    /// Create a new FutureStrategy from a future factory
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
    fn poll_strategy(self: Box<Self>, ctx: &PollContext) -> Self::Stream {
        let factory = self.future_factory;
        let ctx = ctx.clone();

        stream::unfold(ctx, move |ctx| {
            let factory = factory.clone();
            async move {
                let fut = {
                    let mut lock = factory.try_lock().unwrap();
                    (lock)(ctx.worker.clone(), ctx.prev_count.load(Ordering::Relaxed))
                };
                fut.await;
                Some(((), ctx))
            }
        })
        .boxed()
    }
}
