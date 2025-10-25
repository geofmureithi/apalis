use std::{
    collections::{HashMap, VecDeque},
    task::{Context, Poll},
};

use apalis_core::{
    backend::{Backend, TaskSinkError, codec::Codec},
    error::BoxDynError,
    task::{Task, metadata::MetadataExt},
};
use futures::{FutureExt, Sink, TryFutureExt, future::BoxFuture};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use tower::Service;

use crate::{CompositeService, GenerateId, GoTo, StepContext, WorkflowRequest};

#[derive(Debug, Clone, Deserialize)]
pub struct StepResult<T>(pub T);

impl Serialize for StepResult<Vec<u8>> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Try to deserialize the bytes as JSON
        match serde_json::from_slice::<serde_json::Value>(&self.0) {
            Ok(value) => value.serialize(serializer),
            Err(e) => {
                // If deserialization fails, serialize the error
                use serde::ser::Error;
                Err(S::Error::custom(e.to_string()))
            }
        }
    }
}

impl Serialize for StepResult<serde_json::Value> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Try to deserialize the bytes as JSON
        match Value::deserialize(&self.0) {
            Ok(value) => value.serialize(serializer),
            Err(e) => {
                // If deserialization fails, serialize the error
                use serde::ser::Error;
                Err(S::Error::custom(e.to_string()))
            }
        }
    }
}

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
    FlowSink::IdType: GenerateId + Send + 'static,
    Encode: Codec<Compact, Compact = Compact>,
    <FlowSink::Context as MetadataExt<WorkflowRequest>>::Error: Into<BoxDynError>,
    Encode::Error: Into<BoxDynError>,
    FlowSink: Sink<Task<Compact, FlowSink::Context, FlowSink::IdType>, Error = Err> + Unpin,
{
    type Response = StepResult<Compact>;
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

        let has_next = self.services.get(&(idx + 1)).is_some();
        let ctx: StepContext<FlowSink, Encode> =
            StepContext::new(self.backend.clone(), idx, has_next);

        let cl = self
            .services
            .get_mut(&idx)
            .expect("Attempted to run a step that doesn't exist");

        let svc = &mut cl.svc;

        // Prepare the context for the next step
        req.parts.data.insert(ctx);

        self.not_ready.push_back(idx);
        svc.call(req).map_ok(|res| StepResult(res)).boxed()
    }
}

pub async fn handle_workflow_result<N, Compact, FlowSink, Err>(
    ctx: &mut StepContext<FlowSink, FlowSink::Codec>,
    result: &GoTo<N>,
) -> Result<(), TaskSinkError<Err>>
where
    FlowSink: Sink<Task<Compact, FlowSink::Context, FlowSink::IdType>, Error = Err>
        + Backend<Error = Err>
        + Send
        + Unpin,
    FlowSink::Context: MetadataExt<WorkflowRequest>,
    FlowSink::Codec: Codec<N, Compact = Compact>,
    <FlowSink::Codec as Codec<N>>::Error: Into<BoxDynError>,
{
    use futures::SinkExt;
    match result {
        GoTo::Next(next) if ctx.has_next => {
            let task = Task::builder(
                FlowSink::Codec::encode(next).map_err(|e| TaskSinkError::CodecError(e.into()))?,
            )
            .meta(WorkflowRequest {
                step_index: ctx.current_step + 1,
            })
            .build();
            ctx.sink.send(task).await?;
        }
        GoTo::DelayFor(delay, next) if ctx.has_next => {
            let task = Task::builder(
                FlowSink::Codec::encode(next).map_err(|e| TaskSinkError::CodecError(e.into()))?,
            )
            .run_after(*delay)
            .meta(WorkflowRequest {
                step_index: ctx.current_step + 1,
            })
            .build();
            ctx.sink.send(task).await?;
        }
        _ => {}
    }
    Ok(())
}
