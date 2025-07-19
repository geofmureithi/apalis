use futures::future::BoxFuture;
use pin_project_lite::pin_project;
use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};
use tower::{layer::util::Stack, Layer, Service};

use crate::{
    error::{AbortError, BoxDynError, RetryAfterError},
    request::{state::State, Parts, Request},
    worker::builder::WorkerBuilder,
};

use super::long_running::LongRunningLayer;

pub trait AcknowledgementExt<Args, Ctx, Source, Middleware, Ack: Acknowledge<Res, Ctx>, Res>:
    Sized
{
    fn ack_with(
        self,
        ack: Ack,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<AcknowledgeLayer<Ack, Res>, Middleware>>;
}

pub trait Acknowledge<Res, Ctx> {
    type Error;
    type Future: Future<Output = Result<(), Self::Error>>;

    fn ack(&mut self, res: &Result<Res, BoxDynError>, parts: &Parts<Ctx>) -> Self::Future;
}

/// Layer that adds acknowledgment functionality to services
pub struct AcknowledgeLayer<A, Res> {
    acknowledger: A,
    _req: PhantomData<Res>,
}

impl<A, Res> AcknowledgeLayer<A, Res> {
    pub fn new(acknowledger: A) -> Self {
        Self {
            acknowledger,
            _req: PhantomData,
        }
    }
}

impl<S, A, Res> Layer<S> for AcknowledgeLayer<A, Res>
where
    A: Clone,
{
    type Service = AcknowledgeService<S, A, Res>;

    fn layer(&self, inner: S) -> Self::Service {
        AcknowledgeService {
            inner,
            acknowledger: self.acknowledger.clone(),
            _res: PhantomData,
        }
    }
}

/// Service that wraps another service and acknowledges task completion
pub struct AcknowledgeService<S, A, Res> {
    inner: S,
    acknowledger: A,
    _res: PhantomData<Res>,
}

impl<S, A, Args, Ctx> Service<Request<Args, Ctx>> for AcknowledgeService<S, A, S::Response>
where
    S: Service<Request<Args, Ctx>>,
    A: Acknowledge<S::Response, Ctx> + Clone + Send + 'static,
    S::Error: Into<BoxDynError>,
    A::Error: std::error::Error + Send + Sync + 'static,
    S::Future: Send + 'static,
    A::Future: Send + 'static,
    Ctx: Clone + Sync + 'static + Send,
    S::Response: Send,
{
    type Response = S::Response;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<S::Response, BoxDynError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: Request<Args, Ctx>) -> Self::Future {
        let mut parts = req.parts.clone();
        let future = self.inner.call(req);
        let mut acknowledger = self.acknowledger.clone();
        Box::pin(async move {
            let res = future.await.map_err(|e| e.into());
            parts.state = match &res {
                Ok(_) => State::Done,
                Err(_) => State::Failed,
            };
            acknowledger.ack(&res, &parts).await?;
            res
        })
    }
}

impl<Args, P, M, Ctx, Ack, Res> AcknowledgementExt<Args, Ctx, P, M, Ack, Res>
    for WorkerBuilder<Args, Ctx, P, M>
where
    M: Layer<AcknowledgeLayer<Ack, Res>>,
    Ack: Acknowledge<Res, Ctx>,
{
    fn ack_with(
        self,
        ack: Ack,
    ) -> WorkerBuilder<Args, Ctx, P, Stack<AcknowledgeLayer<Ack, Res>, M>> {
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
