use futures_util::future::BoxFuture;
use tower_layer::{Layer, Stack};
use tower_service::Service;
use std::{
    future::Future,
    task::{Context, Poll},
};

use crate::{
    error::BoxDynError,
    task::{status::Status, Metadata, Task},
    worker::builder::WorkerBuilder,
};


pub trait AcknowledgementExt<Args, Ctx, Source, Middleware, Ack: Acknowledge<Res, Ctx>, Res>:
    Sized
{
    fn ack_with(
        self,
        ack: Ack,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<AcknowledgeLayer<Ack>, Middleware>>;
}

pub trait Acknowledge<Res, Ctx> {
    type Error;
    type Future: Future<Output = Result<(), Self::Error>>;

    fn ack(&mut self, res: &Result<Res, BoxDynError>, parts: &Metadata<Ctx>) -> Self::Future;
}

impl<Res, Ctx, F, Fut, E> Acknowledge<Res, Ctx> for F
where
    F: FnMut(&Result<Res, BoxDynError>, &Metadata<Ctx>) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    type Error = E;
    type Future = Fut;

    fn ack(&mut self, res: &Result<Res, BoxDynError>, parts: &Metadata<Ctx>) -> Self::Future {
        (self)(res, parts)
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
pub struct AcknowledgeService<S, A> {
    inner: S,
    acknowledger: A,
}

impl<S, A, Args, Ctx, Res> Service<Task<Args, Ctx>> for AcknowledgeService<S, A>
where
    S: Service<Task<Args, Ctx>, Response = Res>,
    A: Acknowledge<Res, Ctx> + Clone + Send + 'static,
    S::Error: Into<BoxDynError>,
    A::Error: std::error::Error + Send + Sync + 'static,
    S::Future: Send + 'static,
    A::Future: Send + 'static,
    Ctx: Clone + Sync + 'static + Send,
    Res: Send,
{
    type Response = Res;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Res, BoxDynError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: Task<Args, Ctx>) -> Self::Future {
        let parts = req.meta.clone();
        let future = self.inner.call(req);
        let mut acknowledger = self.acknowledger.clone();
        Box::pin(async move {
            let res = future.await.map_err(|e| e.into());
            acknowledger.ack(&res, &parts).await?;
            res
        })
    }
}

impl<Args, P, M, Ctx, Ack, Res> AcknowledgementExt<Args, Ctx, P, M, Ack, Res>
    for WorkerBuilder<Args, Ctx, P, M>
where
    M: Layer<AcknowledgeLayer<Ack>>,
    // M::Service: Service<Request<Args, Ctx>, Response = Res>, 
    Ack: Acknowledge<Res, Ctx>,
{
    fn ack_with(
        self,
        ack: Ack,
    ) -> WorkerBuilder<Args, Ctx, P, Stack<AcknowledgeLayer<Ack>, M>> {
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
