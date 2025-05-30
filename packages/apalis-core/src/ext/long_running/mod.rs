use std::time::Duration;

use tower::{layer::util::Stack, Layer, Service};

use crate::{builder::WorkerBuilder, request::Request};

pub mod tracker;

#[derive(Debug, Default)]
pub struct LongRunningConfig {
    pub max_duration: Option<Duration>,
}
impl LongRunningConfig {
    pub fn new(max_duration: Duration) -> Self {
        Self {
            max_duration: Some(max_duration),
        }
    }
}

pub struct LongRunningLayer;

impl<S> Layer<S> for LongRunningLayer {
    type Service = LongRunningService<S>;

    fn layer(&self, service: S) -> Self::Service {
        LongRunningService { service }
    }
}

pub struct LongRunningService<S> {
    service: S,
}

impl<S, Request> Service<Request> for LongRunningService<S>
where
    S: Service<Request>,
    Request: std::fmt::Debug,
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

    fn call(&mut self, request: Request) -> Self::Future {
        println!("request = {:?}", request);
        self.service.call(request)
    }
}

/// Helper trait for building new Workers from [`WorkerBuilder`]
pub trait LongRunningExt<Req, Source, Middleware>: Sized {
    fn long_running(self) -> WorkerBuilder<Req, Source, Stack<LongRunningLayer, Middleware>> {
        self.long_running_with_cfg(Default::default())
    }
    fn long_running_with_cfg(
        self,
        cfg: LongRunningConfig,
    ) -> WorkerBuilder<Req, Source, Stack<LongRunningLayer, Middleware>>;
}

impl<Args, P, M, Ctx> LongRunningExt<Request<Args, Ctx>, P, M>
    for WorkerBuilder<Request<Args, Ctx>, P, M>
where
    M: Layer<LongRunningLayer>,
{
    fn long_running_with_cfg(
        self,
        cfg: LongRunningConfig,
    ) -> WorkerBuilder<Request<Args, Ctx>, P, Stack<LongRunningLayer, M>> {
        let this = self.layer(LongRunningLayer);
        WorkerBuilder {
            id: this.id,
            request: this.request,
            layer: this.layer,
            source: this.source,
            shutdown: this.shutdown,
            event_handler: this.event_handler,
        }
    }
}
