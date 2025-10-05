use std::{
    collections::{HashMap, VecDeque},
    task::{Context, Poll},
};

use apalis_core::{
    backend::{self, TaskSink, codec::Codec},
    error::BoxDynError,
    task::{Task, metadata::MetadataExt},
};
use futures::future::BoxFuture;
use tower::Service;

use crate::{CompositeService, Step, StepContext, WorkflowRequest};

pub struct WorkFlowService<FlowSink, Encode, Compact>
where
    FlowSink: TaskSink<Compact>,
{
    services: HashMap<usize, CompositeService<FlowSink, Encode, Compact>>,
    not_ready: VecDeque<usize>,
    backend: FlowSink,
}
impl<FlowSink, Encode, Compact> WorkFlowService<FlowSink, Encode, Compact>
where
    FlowSink: TaskSink<Compact>,
{
    pub(crate) fn new(
        services: HashMap<usize, CompositeService<FlowSink, Encode, Compact>>,
        backend: FlowSink,
    ) -> Self {
        Self {
            services,
            not_ready: VecDeque::new(),
            backend,
        }
    }
}

impl<FlowSink: Clone + Send + Sync + 'static + TaskSink<Compact>, Encode, Compact>
    Service<Task<Compact, FlowSink::Context, FlowSink::IdType>>
    for WorkFlowService<FlowSink, Encode, Compact>
where
    FlowSink::Context: MetadataExt<WorkflowRequest>,
    Encode: Send + Sync + 'static,
    Compact: Send + 'static,
    FlowSink: Sync + TaskSink<Compact>,
    Compact: Send + Sync,
    FlowSink::Context: Send + Default + MetadataExt<WorkflowRequest>,
    FlowSink::Error: Into<BoxDynError>,
    FlowSink::IdType: Default + Send + 'static,
    Encode: Codec<Compact, Compact = Compact>,
    <FlowSink::Context as MetadataExt<WorkflowRequest>>::Error: Into<BoxDynError>,
    Encode::Error: Into<BoxDynError>,
{
    type Response = Compact;
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
        let mut ctx: StepContext<FlowSink, Encode> = StepContext::new(self.backend.clone(), idx);
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
            let (should_next, res) = fut.await?;
            if should_next && has_next {
                ctx.push_next_step(&res).await?;
            }
            Ok(res)
        })
    }
}
