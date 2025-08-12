use std::sync::Arc;

use tower_layer::{Layer, Stack};
use tower_service::Service;

use crate::{
    backend::Backend,
    task::Task,
    worker::builder::WorkerBuilder,
    worker::{Event, WorkerContext},
};

pub trait EventListenerExt<Args, Ctx, Source, Middleware>: Sized {
    fn on_event<F: Fn(&WorkerContext, &Event) + Send + Sync + 'static>(
        self,
        f: F,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<EventListenerLayer, Middleware>>;
}

#[derive(Clone)]
pub struct EventListenerLayer;

impl<S> Layer<S> for EventListenerLayer {
    type Service = EventListenerService<S>;

    fn layer(&self, service: S) -> Self::Service {
        EventListenerService { service }
    }
}

pub type EventListener = Arc<Box<dyn Fn(WorkerContext, Event) + Send + Sync>>;

#[derive(Clone)]
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
        // TODO
        // let ctx: &WorkerContext = request.get_checked().unwrap();
        self.service.call(request)
    }
}

impl<Args, P, M, Ctx> EventListenerExt<Args, Ctx, P, M> for WorkerBuilder<Args, Ctx, P, M>
where
    P: Backend<Args, Ctx>,
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
