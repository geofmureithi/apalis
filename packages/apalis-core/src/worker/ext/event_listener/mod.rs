use std::sync::Arc;

use tower_layer::{Layer, Stack};
use tower_service::Service;

use crate::{
    backend::Backend,
    task::Task,
    worker::builder::WorkerBuilder,
    worker::{Event, WorkerContext},
};

/// Worker extension for emitting events 
pub trait EventListenerExt<Args, Meta, Source, Middleware>: Sized {
    fn on_event<F: Fn(&WorkerContext, &Event) + Send + Sync + 'static>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Meta, Source, Stack<EventListenerLayer, Middleware>>;
}

/// Middleware for emitting events
#[derive(Clone)]
pub struct EventListenerLayer;

impl<S> Layer<S> for EventListenerLayer {
    type Service = EventListenerService<S>;

    fn layer(&self, service: S) -> Self::Service {
        EventListenerService { service }
    }
}

/// Event listening type
pub type EventListener = Arc<Box<dyn Fn(WorkerContext, Event) + Send + Sync>>;


/// Service for emitting events
#[derive(Clone)]
pub struct EventListenerService<S> {
    service: S,
}

impl<S, Args, Meta, IdType> Service<Task<Args, Meta, IdType>> for EventListenerService<S>
where
    S: Service<Task<Args, Meta, IdType>>,
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

    fn call(&mut self, request: Task<Args, Meta, IdType>) -> Self::Future {
        self.service.call(request)
    }
}

impl<Args, P, M, Meta> EventListenerExt<Args, Meta, P, M> for WorkerBuilder<Args, Meta, P, M>
where
    P: Backend<Args, Meta>,
    M: Layer<EventListenerLayer>,
{
    fn on_event<F: Fn(&WorkerContext, &Event) + Send + Sync + 'static>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Meta, P, Stack<EventListenerLayer, M>> {
        let new_fn = self
            .event_handler
            .write()
            .map(|mut res| {
                let current = res.take();
                match current {
                    Some(c) => {
                        let new: Box<dyn Fn(&WorkerContext, &Event) + Send + Sync + 'static> =
                            Box::new(move |ctx, ev| {
                                c(&ctx, &ev);
                                f(&ctx, &ev);
                            });
                        new
                    }
                    None => {
                        let new: Box<dyn Fn(&WorkerContext, &Event) + Send + Sync + 'static> =
                            Box::new(move |ctx, ev| {
                                f(&ctx, &ev);
                            });
                        new
                    }
                }
            })
            .unwrap();
        let _ = self.event_handler.write().map(|mut res| {
            let _ = res.insert(new_fn);
        });
        self.layer(EventListenerLayer)
    }
}
