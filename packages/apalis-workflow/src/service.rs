use std::{
    collections::{HashMap, VecDeque},
    task::{Context, Poll},
};

use apalis_core::{
    backend::{Backend, codec::Codec},
    error::BoxDynError,
    task::{Task, metadata::MetadataExt},
};
use futures::{Sink, future::BoxFuture};
use tower::Service;

use crate::{CompositeService, GoTo, StepContext, WorkflowRequest};

pub struct WorkFlowService<FlowSink, Encode, Compact, Context, IdType> {
    services: HashMap<usize, CompositeService<FlowSink, Encode, Compact, Context, IdType>>,
    not_ready: VecDeque<usize>,
    backend: FlowSink,
}
impl<FlowSink, Encode, Compact, Context, IdType>
    WorkFlowService<FlowSink, Encode, Compact, Context, IdType>
{
    pub(crate) fn new(
        services: HashMap<usize, CompositeService<FlowSink, Encode, Compact, Context, IdType>>,
        backend: FlowSink,
    ) -> Self {
        Self {
            services,
            not_ready: VecDeque::new(),
            backend,
        }
    }
}

impl<FlowSink: Clone + Send + Sync + 'static + Backend<Error = Err>, Encode, Compact, Err>
    Service<Task<Compact, FlowSink::Context, FlowSink::IdType>>
    for WorkFlowService<FlowSink, Encode, Compact, FlowSink::Context, FlowSink::IdType>
where
    FlowSink::Context: MetadataExt<WorkflowRequest>,
    Encode: Send + Sync + 'static,
    Compact: Send + 'static + Clone,
    FlowSink: Sync,
    Compact: Send + Sync,
    FlowSink::Context: Send + Default + MetadataExt<WorkflowRequest>,
    Err: std::error::Error + Send + Sync + 'static,
    FlowSink::IdType: Default + Send + 'static,
    Encode: Codec<Compact, Compact = Compact>,
    <FlowSink::Context as MetadataExt<WorkflowRequest>>::Error: Into<BoxDynError>,
    Encode::Error: Into<BoxDynError>,
    FlowSink: Sink<Task<Compact, FlowSink::Context, FlowSink::IdType>, Error = Err> + Unpin,
{
    type Response = GoTo<Compact>;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            // must wait for *all* services to be ready.
            // this will cause head-of-line blocking unless the underlying services are always ready.
            if self.not_ready.is_empty() {
                return Poll::Ready(Ok(()));
            } else {
                if self
                    .services
                    .get_mut(&self.not_ready[0])
                    .unwrap()
                    .svc
                    .poll_ready(cx)?
                    .is_pending()
                {
                    return Poll::Pending;
                }

                self.not_ready.pop_front();
            }
        }
    }

    fn call(
        &mut self,
        mut req: Task<Compact, FlowSink::Context, FlowSink::IdType>,
    ) -> Self::Future {
        assert!(
            self.not_ready.is_empty(),
            "Workflow must wait for all services to be ready. Did you forget to call poll_ready()?"
        );
        let meta: WorkflowRequest = req.parts.ctx.extract().unwrap_or_default();
        let idx = meta.step_index;
        let ctx: StepContext<FlowSink, Encode> = StepContext::new(self.backend.clone(), idx);
        let mut sink = self.backend.clone();
        let has_next = self.services.get(&(idx + 1)).is_some();

        let cl = self
            .services
            .get_mut(&idx)
            .expect("Attempted to run a step that doesn't exist");

        let svc = &mut cl.svc;

        req.parts.data.insert(ctx.clone());

        self.not_ready.push_back(idx);
        let fut = svc.call(req);
        Box::pin(async move {
            let res = fut.await?;
            match &res {
                GoTo::Next(next) if has_next => {
                    use futures::SinkExt;
                    let task = Task::builder(next.clone())
                        .meta(WorkflowRequest {
                            step_index: idx + 1,
                        })
                        .build();
                    sink.send(task).await?;
                }
                GoTo::DelayFor(delay, next) if has_next => {
                    use futures::SinkExt;
                    let task = Task::builder(next.clone())
                        .run_after(*delay)
                        .meta(WorkflowRequest {
                            step_index: idx + 1,
                        })
                        .build();
                    sink.send(task).await?;
                }
                _ => {}
            }
            Ok(res)
        })
    }
}
