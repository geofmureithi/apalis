use std::{
    any::Any,
    collections::{HashMap, VecDeque},
    task::{Context, Poll},
};

use apalis_core::{
    backend::{Backend, TaskSinkError, codec::Codec},
    error::BoxDynError,
    task::{Task, metadata::MetadataExt},
};
use futures::{FutureExt, Sink, TryFutureExt, future::BoxFuture};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use tower::Service;

use crate::{CompositeService, GenerateId, GoTo, Step, StepContext, WorkflowContext};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepResult<T>(pub T);

// impl Serialize for StepResult<Vec<u8>> {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         // Try to deserialize the bytes as JSON
//         match serde_json::from_slice::<serde_json::Value>(&self.0) {
//             Ok(value) => value.serialize(serializer),
//             Err(e) => {
//                 // If deserialization fails, serialize the error
//                 use serde::ser::Error;
//                 Err(S::Error::custom(e.to_string()))
//             }
//         }
//     }
// }

pub struct WorkflowCodec<C> {
    _marker: std::marker::PhantomData<C>,
}

impl<T: Any, C> Codec<T> for WorkflowCodec<C>
where
    C: Codec<T>,
    C::Compact: Clone + 'static,
{
    type Error = C::Error;

    type Compact = C::Compact;

    fn encode(item: &T) -> Result<Self::Compact, Self::Error> {
        dbg!("Encoding item in WorkflowCodec {}", std::any::type_name::<T>());
        if let Some(step_result) = (item as &dyn Any).downcast_ref::<C::Compact>() {
            println!("Encoding Compact StepResult with inner value");
            Ok(step_result.clone())
        } else if let Some(step_result) =
            (item as &dyn Any).downcast_ref::<StepResult<C::Compact>>()
        {
            println!("Encoding StepResult with inner value");
            Ok(step_result.0.clone())
        } else {
            println!("Encoding regular item");
            C::encode(item)
        }
    }

    fn decode(data: &Self::Compact) -> Result<T, Self::Error> {
        unimplemented!("WorkflowCodec does not support decoding directly")
    }
}

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct StepRequest<T>(pub T);

// impl<'de> Deserialize<'de> for StepRequest<Vec<u8>> {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         let json = Value::deserialize(deserializer)?;
//         let bytes = serde_json::to_vec(&json).map_err(serde::de::Error::custom)?;
//         Ok(StepRequest(bytes))
//     }
// }

pub struct WorkflowService<FlowSink, Encode, Compact, Context, IdType> {
    services: HashMap<usize, CompositeService<FlowSink, Encode, Compact, Context, IdType>>,
    not_ready: VecDeque<usize>,
    backend: FlowSink,
}
impl<FlowSink, Encode, Compact, Context, IdType>
    WorkflowService<FlowSink, Encode, Compact, Context, IdType>
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
    for WorkflowService<FlowSink, Encode, Compact, FlowSink::Context, FlowSink::IdType>
where
    FlowSink::Context: MetadataExt<WorkflowContext>,
    Encode: Send + Sync + 'static,
    Compact: Send + 'static + Clone,
    FlowSink: Sync,
    Compact: Send + Sync,
    FlowSink::Context: Send + Default + MetadataExt<WorkflowContext>,
    Err: std::error::Error + Send + Sync + 'static,
    FlowSink::IdType: GenerateId + Send + 'static,
    Encode: Codec<Compact, Compact = Compact>,
    <FlowSink::Context as MetadataExt<WorkflowContext>>::Error: Into<BoxDynError>,
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
        let meta: WorkflowContext = req.parts.ctx.extract().unwrap_or_default();
        let idx = meta.step_index;

        let has_next = self.services.contains_key(&(idx + 1));
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

/// Handle the result of a workflow step, scheduling the next step if necessary
pub async fn handle_workflow_result<N, Compact, FlowSink, Err>(
    ctx: &mut StepContext<FlowSink, FlowSink::Codec>,
    result: &GoTo<N>,
) -> Result<(), TaskSinkError<Err>>
where
    FlowSink: Sink<Task<Compact, FlowSink::Context, FlowSink::IdType>, Error = Err>
        + Backend<Error = Err>
        + Send
        + Unpin,
    FlowSink::Context: MetadataExt<WorkflowContext>,
    FlowSink::Codec: Codec<N, Compact = Compact>,
    <FlowSink::Codec as Codec<N>>::Error: Into<BoxDynError>,
    Compact: Clone + 'static,
    N: 'static,
{
    use futures::SinkExt;
    match result {
        GoTo::Next(next) if ctx.has_next => {
            let task = Task::builder(
                WorkflowCodec::<FlowSink::Codec>::encode(next)
                    .map_err(|e| TaskSinkError::CodecError(e.into()))?,
            )
            .meta(WorkflowContext {
                step_index: ctx.current_step + 1,
            })
            .build();
            ctx.sink.send(task).await?;
        }
        GoTo::DelayFor(delay, next) if ctx.has_next => {
            let task = Task::builder(
                WorkflowCodec::<FlowSink::Codec>::encode(next)
                    .map_err(|e| TaskSinkError::CodecError(e.into()))?,
            )
            .run_after(*delay)
            .meta(WorkflowContext {
                step_index: ctx.current_step + 1,
            })
            .build();
            ctx.sink.send(task).await?;
        }
        _ => {}
    }
    Ok(())
}
