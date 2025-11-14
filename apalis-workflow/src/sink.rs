use apalis_core::{
    backend::{BackendExt, TaskSinkError, codec::Codec},
    error::BoxDynError,
    task::{Task, builder::TaskBuilder, metadata::MetadataExt, task_id::TaskId},
};
use futures::Sink;

use crate::{context::WorkflowContext, id_generator::GenerateId};

/// Extension trait for pushing tasks into a workflow
pub trait WorkflowSink<Args>: BackendExt
where
    Self::Codec: Codec<Args, Compact = Self::Compact>,
{
    /// Push a step into the workflow sink at the start
    fn push_start(
        &mut self,
        step: Args,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send {
        self.push_step(step, 0)
    }

    /// Push a step into the workflow sink at the specified index
    fn push_step(
        &mut self,
        step: Args,
        index: usize,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;
}

impl<S: Send, Args: Send, Compact, Err> WorkflowSink<Args> for S
where
    S: Sink<Task<Compact, S::Context, S::IdType>, Error = Err>
        + BackendExt<Error = Err, Compact = Compact>
        + Unpin,
    S::IdType: GenerateId + Send,
    S::Codec: Codec<Args, Compact = Compact>,
    S::Context: MetadataExt<WorkflowContext> + Send,
    Err: std::error::Error + Send + Sync + 'static,
    <S::Codec as Codec<Args>>::Error: Into<BoxDynError> + Send + Sync + 'static,
    <S::Context as MetadataExt<WorkflowContext>>::Error: Into<BoxDynError> + Send + Sync + 'static,
    Compact: Send + 'static,
{
    async fn push_step(
        &mut self,
        step: Args,
        index: usize,
    ) -> Result<(), TaskSinkError<Self::Error>> {
        use futures::SinkExt;
        let task_id = TaskId::new(S::IdType::generate());
        let compact = S::Codec::encode(&step).map_err(|e| TaskSinkError::CodecError(e.into()))?;
        let task = TaskBuilder::new(compact)
            .meta(WorkflowContext { step_index: index })
            .with_task_id(task_id.clone())
            .build();
        self.send(task)
            .await
            .map_err(|e| TaskSinkError::PushError(e))
    }
}
