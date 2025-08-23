use futures_util::future::BoxFuture;
use std::{
    future::Future,
    task::Poll,
};
use tower_layer::{Layer, Stack};
use tower_service::Service;

use crate::{
    backend::Backend,
    error::BoxDynError,
    task::{ExecutionContext, Task},
    worker::{builder::WorkerBuilder, context::WorkerContext},
};

/// 
pub trait AcknowledgementExt<Args, Meta, Source, Middleware, Ack, Res>: Sized
where
    Source: Backend<Args, Meta>,
    Ack: Acknowledge<Res, Meta, Source::IdType>,
{
    fn ack_with(
        self,
        ack: Ack,
    ) -> WorkerBuilder<Args, Meta, Source, Stack<AcknowledgeLayer<Ack>, Middleware>>;
}

pub trait Acknowledge<Res, Meta, IdType> {
    type Error;
    type Future: Future<Output = Result<(), Self::Error>>;

    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        ctx: &ExecutionContext<Meta, IdType>,
    ) -> Self::Future;
}

impl<Res, Meta, F, Fut, IdType, E> Acknowledge<Res, Meta, IdType> for F
where
    F: FnMut(&Result<Res, BoxDynError>, &ExecutionContext<Meta, IdType>) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    type Error = E;
    type Future = Fut;

    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        ctx: &ExecutionContext<Meta, IdType>,
    ) -> Self::Future {
        (self)(res, ctx)
    }
}

/// Layer that adds acknowledgment functionality to services
pub struct AcknowledgeLayer<A> {
    acknowledger: A,
}

impl<A> AcknowledgeLayer<A> {
    pub fn new(acknowledger: A) -> Self {
        Self { acknowledger }
    }
}

impl<S, A> Layer<S> for AcknowledgeLayer<A>
where
    A: Clone,
{
    type Service = AcknowledgeService<S, A>;

    fn layer(&self, inner: S) -> Self::Service {
        AcknowledgeService {
            inner,
            acknowledger: self.acknowledger.clone(),
        }
    }
}

/// Service that wraps another service and acknowledges task completion
#[derive(Debug, Clone)]
pub struct AcknowledgeService<S, A> {
    inner: S,
    acknowledger: A,
}

impl<S, A, Args, Meta, Res, IdType> Service<Task<Args, Meta, IdType>> for AcknowledgeService<S, A>
where
    S: Service<Task<Args, Meta, IdType>, Response = Res>,
    A: Acknowledge<Res, Meta, IdType> + Clone + Send + 'static,
    S::Error: Into<BoxDynError>,
    A::Error: std::error::Error + Send + Sync + 'static,
    S::Future: Send + 'static,
    A::Future: Send + 'static,
    Meta: Clone + Sync + 'static + Send,
    Res: Send,
    IdType: Send + Clone + 'static,
{
    type Response = Res;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Res, BoxDynError>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: Task<Args, Meta, IdType>) -> Self::Future {
        let parts = req.ctx.clone();
        let worker: WorkerContext = req.get().cloned().unwrap();
        let future = self.inner.call(req);
        let mut acknowledger = self.acknowledger.clone();
        Box::pin(async move {
            let res = future.await.map_err(|e| e.into());
            worker.track(acknowledger.ack(&res, &parts)).await?; // Ensure ack is gracefully shutdown
            res
        })
    }
}

impl<Args, B, M, Meta, Ack, Res> AcknowledgementExt<Args, Meta, B, M, Ack, Res>
    for WorkerBuilder<Args, Meta, B, M>
where
    M: Layer<AcknowledgeLayer<Ack>>,
    Ack: Acknowledge<Res, Meta, B::IdType>,
    B: Backend<Args, Meta>,
{
    fn ack_with(self, ack: Ack) -> WorkerBuilder<Args, Meta, B, Stack<AcknowledgeLayer<Ack>, M>> {
        let this = self.layer(AcknowledgeLayer::new(ack));
        WorkerBuilder {
            name: this.name,
            request: this.request,
            layer: this.layer,
            source: this.source,
            shutdown: this.shutdown,
            event_handler: this.event_handler,
        }
    }
}
