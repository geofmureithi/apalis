//! Worker extension for emitting events.
//!
//! The [`EventListenerExt`] trait allows you to register callbacks for worker events.
//! It also provides the [`EventListenerLayer`] middleware for emitting events and the [`EventListenerService`] service.
//!
//! # Example
//!
//! ```rust,no_run
//! # use apalis_core::worker::ext::event_listener::EventListenerExt;
//! # use apalis_core::worker::{builder::WorkerBuilder, event::Event, context::WorkerContext};
//! # use apalis_core::backend::memory::MemoryStorage;
//! # let in_memory: MemoryStorage<()> = MemoryStorage::new();
//!
//! let builder = WorkerBuilder::new("my-worker")
//! #   .backend(in_memory)
//!     .on_event(|ctx: &WorkerContext, event: &Event| {
//!         println!("Received event: {:?}", event);
//!     });
//! ```
//!
//! This will print every event emitted by the worker.
//!
use tower_layer::{Layer, Stack};
use tower_service::Service;

use crate::{
    backend::Backend,
    task::Task,
    worker::{Event, WorkerContext, builder::WorkerBuilder, event::RawEventListener},
};

/// Worker extension for emitting events
pub trait EventListenerExt<Args, Ctx, Source, Middleware>: Sized {
    /// Register a callback for worker events
    fn on_event<F: Fn(&WorkerContext, &Event) + Send + Sync + 'static>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<EventListenerLayer, Middleware>>;
}

/// Middleware for emitting events
#[derive(Debug, Clone, Default)]
pub struct EventListenerLayer(());

impl EventListenerLayer {
    /// Create a new event listener layer
    pub fn new() -> Self {
        Self::default()
    }
}

impl<S> Layer<S> for EventListenerLayer {
    type Service = EventListenerService<S>;

    fn layer(&self, service: S) -> Self::Service {
        EventListenerService { service }
    }
}

/// Service for emitting events
#[derive(Debug, Clone)]
pub struct EventListenerService<S> {
    service: S,
}

impl<S, Args, Ctx, IdType> Service<Task<Args, Ctx, IdType>> for EventListenerService<S>
where
    S: Service<Task<Args, Ctx, IdType>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Task<Args, Ctx, IdType>) -> Self::Future {
        self.service.call(request)
    }
}

impl<Args, P, M, Ctx> EventListenerExt<Args, Ctx, P, M> for WorkerBuilder<Args, Ctx, P, M>
where
    P: Backend<Args = Args, Context = Ctx>,
    M: Layer<EventListenerLayer>,
{
    fn on_event<F: Fn(&WorkerContext, &Event) + Send + Sync + 'static>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Ctx, P, Stack<EventListenerLayer, M>> {
        let new_fn = self
            .event_handler
            .write()
            .map(|mut res| {
                let current = res.take();
                match current {
                    Some(c) => {
                        let new: RawEventListener = Box::new(move |ctx, ev| {
                            c(ctx, ev);
                            f(ctx, ev);
                        });
                        new
                    }
                    None => {
                        let new: RawEventListener = Box::new(move |ctx, ev| {
                            f(ctx, ev);
                        });
                        new
                    }
                }
            })
            .unwrap();
        let _ = self.event_handler.write().map(|mut res| {
            let _ = res.insert(new_fn);
        });
        self.layer(EventListenerLayer::new())
    }
}
