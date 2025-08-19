use std::{future::Future, ops::Deref, time::Duration};

use futures_util::{future::BoxFuture, FutureExt};
use tower_layer::{Layer, Stack};
use tower_service::Service;

use crate::{
    backend::Backend,
    service_fn::from_request::FromRequest,
    task::{data::MissingDataError, Task},
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

pub struct LongRunnerCtx {
    tracker: TaskTracker,
    wrk: WorkerContext,
}

impl LongRunnerCtx {
    /// Start a task that is tracked by the long running task's context
    pub fn track<F: Future>(&self, task: F) -> Tracked<TrackedFuture<F>> {
        self.wrk.track(self.tracker.track_future(task))
    }
}

/// CTX: FromRequest<Request<Args, Ctx>>
impl<Args: Sync, Meta: Sync + Clone, IdType: Sync + Send> FromRequest<Task<Args, Meta, IdType>>
    for LongRunnerCtx
{
    type Error = MissingDataError;
    async fn from_request(req: &Task<Args, Meta, IdType>) -> Result<Self, Self::Error> {
        let tracker: &TaskTracker = req.get_checked()?;
        let wrk: &WorkerContext = req.get_checked()?;
        Ok(LongRunnerCtx {
            tracker: tracker.clone(),
            wrk: wrk.clone(),
        })
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

impl<S, Args, Meta, IdType> Service<Task<Args, Meta, IdType>> for LongRunningService<S>
where
    S: Service<Task<Args, Meta, IdType>>,
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

    fn call(&mut self, mut request: Task<Args, Meta, IdType>) -> Self::Future {
        let tracker = TaskTracker::new();
        request.insert(tracker.clone());
        let worker: WorkerContext = request.get().cloned().unwrap();
        let req = self.service.call(request);
        let fut = async move {
            let res = req.await;
            tracker.close();
            let tracker_fut = worker.track(tracker.wait()); // Long running tasks will be awaited in a shutdown
            tracker_fut.await;
            res
        }
        .boxed();
        fut
    }
}

/// Helper trait for building new Workers from [`WorkerBuilder`]
pub trait LongRunningExt<Args, Meta, Source, Middleware>: Sized {
    fn long_running(self) -> WorkerBuilder<Args, Meta, Source, Stack<LongRunningLayer, Middleware>> {
        self.long_running_with_cfg(Default::default())
    }
    fn long_running_with_cfg(
        self,
        cfg: LongRunningConfig,
    ) -> WorkerBuilder<Args, Meta, Source, Stack<LongRunningLayer, Middleware>>;
}

impl<Args, B, M, Meta> LongRunningExt<Args, Meta, B, M> for WorkerBuilder<Args, Meta, B, M>
where
    M: Layer<LongRunningLayer>,
    B: Backend<Args, Meta>,
{
    fn long_running_with_cfg(
        self,
        cfg: LongRunningConfig,
    ) -> WorkerBuilder<Args, Meta, B, Stack<LongRunningLayer, M>> {
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

    use crate::{
        backend::{memory::MemoryStorage, Backend, BackendWithSink, TaskSink},
        error::BoxDynError,
        service_fn::{self, service_fn, ServiceFn},
        task::data::Data,
        worker::{
            builder::WorkerBuilder,
            context::WorkerContext,
            ext::{event_listener::EventListenerExt, long_running::LongRunningExt},
        },
    };

    use super::*;

    const ITEMS: u32 = 1_000_000;

    #[tokio::test]
    async fn basic_worker() {
        let mut in_memory = MemoryStorage::new();
        let mut sink = in_memory.sink();
        for i in 0..ITEMS {
            sink.push(i).await.unwrap();
        }

        async fn task(
            task: u32,
            runner: LongRunnerCtx,
            worker: WorkerContext,
        ) -> Result<(), BoxDynError> {
            tokio::spawn(runner.track(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }));
            if task == ITEMS - 1 {
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    worker.stop().unwrap();
                });
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .long_running()
            .on_event(|ctx, ev| {
                // println!("On Event = {:?}, {:?}", ev, ctx);
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
