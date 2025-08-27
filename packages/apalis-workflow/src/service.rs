use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    marker::PhantomData,
    task::{Context, Poll},
};

use apalis_core::{
    backend::{self, TaskSink},
    error::BoxDynError,
    task::{metadata::MetadataExt, Task},
};
use futures::future::BoxFuture;
use serde_json::Value;
use tower::Service;

use crate::{CompositeService, StepContext, SteppedService, WorkflowRequest};

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
    Service<Task<Compact, FlowSink::Meta, FlowSink::IdType>>
    for WorkFlowService<FlowSink, Encode, Compact>
where
    FlowSink::Meta: MetadataExt<WorkflowRequest>,
    Encode: Send + Sync + 'static,
    Compact: Send + 'static,
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

    fn call(&mut self, mut req: Task<Compact, FlowSink::Meta, FlowSink::IdType>) -> Self::Future {
        assert!(
            self.not_ready.is_empty(),
            "Workflow must wait for all services to be ready. Did you forget to call poll_ready()?"
        );
        let meta: WorkflowRequest = req.ctx.metadata.extract().unwrap_or_default();
        let idx = meta.step_index;
        let ctx = StepContext::new(self.backend.clone(), idx);

        let next_hook = self.services.get(&(&idx + 1)).map(|s| s.pre_hook.clone());

        let cl = self
            .services
            .get_mut(&idx)
            .expect("Attempted to run a step that doesn't exist");
        let svc = &mut cl.svc;

        req.insert(ctx.clone());

        self.not_ready.push_back(idx);
        let fut = svc.call(req);
        Box::pin(async move {
            let (should_next, res) = fut.await?;
            if let Some(next_hook) = next_hook {
                if should_next {
                    next_hook(&ctx, &res).await?;
                }
            }
            Ok(res)
        })
    }
}
