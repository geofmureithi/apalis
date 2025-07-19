use std::{future::Future, ops::Deref, time::Duration};

use futures::{future::BoxFuture, FutureExt};
use tower::{layer::util::Stack, Layer, Service};

use crate::{
    request::{data::MissingDataError, Request},
    service_fn::from_request::FromRequest,
    worker::{
        builder::WorkerBuilder,
        context::{Tracked, WorkerContext},
        ext::long_running::tracker::{TaskTracker, TrackedFuture},
    },
};

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

pub struct LongRunner<Ctx> {
    ctx: Ctx,
    tracker: TaskTracker,
    wrk: WorkerContext,
}

impl<Ctx> LongRunner<Ctx> {
    /// Start a task that is tracked by the long running task's context
    pub fn track<F: Future>(&self, task: F) -> Tracked<TrackedFuture<F>> {
        self.wrk.track(self.tracker.track_future(task))
    }
}

/// CTX: FromRequest<Request<Args, Ctx>>
impl<Args: Sync, Ctx: Sync + Clone> FromRequest<Request<Args, Ctx>> for LongRunner<Ctx> {
    type Error = MissingDataError;
    async fn from_request(req: &Request<Args, Ctx>) -> Result<Self, Self::Error> {
        let ctx = req.parts.context.clone();
        let tracker: &TaskTracker = req.get_checked()?;
        let wrk: &WorkerContext = req.get_checked()?;
        Ok(LongRunner {
            ctx,
            tracker: tracker.clone(),
            wrk: wrk.clone(),
        })
    }
}

impl<Ctx> Deref for LongRunner<Ctx> {
    type Target = Ctx;

    fn deref(&self) -> &Self::Target {
        &self.ctx
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

impl<S, Args, Ctx> Service<Request<Args, Ctx>> for LongRunningService<S>
where
    S: Service<Request<Args, Ctx>>,
    S::Future: Send + 'static,
    S::Response: Send,
    S::Error: Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<S::Response, S::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<Args, Ctx>) -> Self::Future {
        let tracker = TaskTracker::new();
        request.insert(tracker.clone());

        let req = self.service.call(request);
        let fut = async move {
            let res = req.await;
            tracker.close();
            let tracker_fut = tracker.wait().boxed();
            tracker_fut.await;
            res
        }
        .boxed();
        fut
    }
}

/// Helper trait for building new Workers from [`WorkerBuilder`]
pub trait LongRunningExt<Args, Ctx, Source, Middleware>: Sized {
    fn long_running(self) -> WorkerBuilder<Args, Ctx, Source, Stack<LongRunningLayer, Middleware>> {
        self.long_running_with_cfg(Default::default())
    }
    fn long_running_with_cfg(
        self,
        cfg: LongRunningConfig,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<LongRunningLayer, Middleware>>;
}

impl<Args, P, M, Ctx> LongRunningExt<Args, Ctx, P, M> for WorkerBuilder<Args, Ctx, P, M>
where
    M: Layer<LongRunningLayer>,
{
    fn long_running_with_cfg(
        self,
        cfg: LongRunningConfig,
    ) -> WorkerBuilder<Args, Ctx, P, Stack<LongRunningLayer, M>> {
        let this = self.layer(LongRunningLayer);
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

#[cfg(test)]
mod tests {
    use std::{ops::Deref, sync::atomic::AtomicUsize, time::Duration};

    use futures::channel::oneshot::channel;

    use crate::{
        backend::{memory::MemoryStorage, Backend, TaskSink},
        error::BoxDynError,
        request::data::Data,
        service_fn::{self, service_fn, ServiceFn},
        worker::{
            builder::WorkerBuilder,
            context::WorkerContext,
            ext::{event_listener::EventListenerExt, long_running::LongRunningExt},
        },
    };

    use super::*;

    const ITEMS: u32 = 2;

    #[tokio::test]
    async fn basic_worker() {
        let in_memory = MemoryStorage::new();
        let mut sink = in_memory.sink();
        for i in 0..ITEMS {
            sink.push(i).await.unwrap();
        }

        async fn task(
            task: u32,
            runner: LongRunner<()>,
            worker: WorkerContext,
        ) -> Result<&'static str, BoxDynError> {
            let (tx, rx) = channel();
            tokio::spawn(runner.track(async move {
                tokio::time::sleep(Duration::from_secs(task as _)).await;
                tx.send("Completed").unwrap();
            }));
            if task == ITEMS - 1 {
                tokio::spawn(runner.track(async move {
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    worker.stop().unwrap();
                }));
            }
            Ok(rx.await?)
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .long_running()
            .on_event(|_ctx, ev| {
                println!("On Event = {:?}", ev);
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
