use crate::{
    backend::{Backend, codec::Codec},
    error::BoxDynError,
    task::Task,
};
use futures_core::Stream;
use futures_sink::Sink;
use futures_util::SinkExt;
use futures_util::StreamExt;
use futures_util::stream;

/// Error type for TaskSink operations
#[derive(Debug, thiserror::Error)]
pub enum TaskSinkError<PushError> {
    /// Error occurred while pushing the task
    #[error("Failed to push task: {0}")]
    PushError(#[from] PushError),
    /// Error occurred during encoding/decoding of the task
    #[error("Failed to encode/decode task: {0}")]
    CodecError(BoxDynError),
}

/// Extends Backend to allow pushing tasks into the backend
pub trait TaskSink<Args>: Backend {
    /// Allows pushing a single task into the backend
    fn push(
        &mut self,
        task: Args,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;

    /// Allows pushing multiple tasks into the backend in bulk
    fn push_bulk(
        &mut self,
        tasks: Vec<Args>,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;

    /// Allows pushing tasks from a stream into the backend
    fn push_stream(
        &mut self,
        tasks: impl Stream<Item = Args> + Unpin + Send,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;

    /// Allows pushing a fully constructed task into the backend
    fn push_task(
        &mut self,
        task: Task<Args, Self::Context, Self::IdType>,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;

    /// Allows pushing a fully constructed task into the backend
    fn push_all(
        &mut self,
        tasks: impl Stream<Item = Task<Args, Self::Context, Self::IdType>> + Unpin + Send,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;
}

impl<Args, S, E, C> TaskSink<Args> for S
where
    S: Sink<Task<C::Compact, Self::Context, Self::IdType>, Error = E>
        + Unpin
        + Backend<Args = Args, Error = E, Codec = C>
        + Send,
    Args: Send,
    C::Compact: Send,
    S::Context: Send + Default,
    S::IdType: Send + 'static,
    C: Codec<Args>,
    E: Send,
    C::Error: std::error::Error + Send + Sync + 'static,
{
    async fn push(&mut self, task: Args) -> Result<(), TaskSinkError<Self::Error>> {
        use futures_util::SinkExt;
        let encoded = C::encode(&task).map_err(|e| TaskSinkError::CodecError(e.into()))?;
        self.send(Task::new(encoded)).await?;
        Ok(())
    }

    async fn push_bulk(&mut self, tasks: Vec<Args>) -> Result<(), TaskSinkError<Self::Error>> {
        use futures_util::SinkExt;
        let tasks = tasks
            .into_iter()
            .map(Task::new)
            .map(|task| {
                task.try_map(|t| C::encode(&t).map_err(|e| TaskSinkError::CodecError(e.into())))
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.send_all(&mut stream::iter(tasks.into_iter().map(Ok)))
            .await?;
        Ok(())
    }

    async fn push_stream(
        &mut self,
        tasks: impl Stream<Item = Args> + Unpin + Send,
    ) -> Result<(), TaskSinkError<Self::Error>> {
        Ok(self
            .sink_map_err(|e| TaskSinkError::PushError(e))
            .send_all(&mut tasks.map(Task::new).map(|task| {
                task.try_map(|t| C::encode(&t).map_err(|e| TaskSinkError::CodecError(e.into())))
            }))
            .await?)
    }

    async fn push_task(
        &mut self,
        task: Task<Args, Self::Context, Self::IdType>,
    ) -> Result<(), TaskSinkError<Self::Error>> {
        use futures_util::SinkExt;
        self.sink_map_err(|e| TaskSinkError::PushError(e))
            .send(task.try_map(|t| C::encode(&t).map_err(|e| TaskSinkError::CodecError(e.into())))?)
            .await
    }

    async fn push_all(
        &mut self,
        tasks: impl Stream<Item = Task<Args, Self::Context, Self::IdType>> + Unpin + Send,
    ) -> Result<(), TaskSinkError<Self::Error>> {
        use futures_util::SinkExt;
        self.sink_map_err(|e| TaskSinkError::PushError(e))
            .send_all(&mut tasks.map(|task| {
                task.try_map(|t| C::encode(&t).map_err(|e| TaskSinkError::CodecError(e.into())))
            }))
            .await
    }
}

/// Extends Backend to allow pushing tasks into the backend without requiring compile time constraints.
/// By default the TaskSink trait requires `TaskSink<Args>: Backend<Args>` which can be restrictive in certain scenarios.
/// This means you cannot push tasks of different argument types using the same backend instance.
/// The WeakTaskSink trait relaxes this constraint, allowing pushing tasks of any argument type as long as they match the backend's context and ID type.
/// This is useful for dynamic task management scenarios where tasks of varying types need to be pushed into the same backend instance.
pub trait WeakTaskSink<Args>: Backend {
    /// Allows pushing a single task into the backend
    fn push(
        &mut self,
        task: Args,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;

    /// Allows pushing multiple tasks into the backend in bulk
    fn push_bulk(
        &mut self,
        tasks: Vec<Args>,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;

    /// Allows pushing tasks from a stream into the backend
    fn push_stream(
        &mut self,
        tasks: impl Stream<Item = Args> + Unpin + Send,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;

    /// Allows pushing a fully constructed task into the backend
    fn push_task(
        &mut self,
        task: Task<Args, Self::Context, Self::IdType>,
    ) -> impl Future<Output = Result<(), TaskSinkError<Self::Error>>> + Send;
}

impl<Args, S, E, C> WeakTaskSink<Args> for S
where
    S: Sink<Task<C::Compact, Self::Context, Self::IdType>, Error = E>
        + Unpin
        + Backend<Error = E, Codec = C> // Note: No Args constraint here
        + Send,
    Args: Send,
    C::Compact: Send,
    S::Context: Send + Default,
    S::IdType: Send + 'static,
    C: Codec<Args>,
    E: Send,
    C::Error: std::error::Error + Send + Sync + 'static,
{
    async fn push(&mut self, task: Args) -> Result<(), TaskSinkError<Self::Error>> {
        use futures_util::SinkExt;
        let encoded = C::encode(&task).map_err(|e| TaskSinkError::CodecError(e.into()))?;
        self.sink_map_err(|e| TaskSinkError::PushError(e))
            .send(Task::new(encoded))
            .await
    }

    async fn push_bulk(&mut self, tasks: Vec<Args>) -> Result<(), TaskSinkError<Self::Error>> {
        use futures_util::SinkExt;
        self.sink_map_err(|e| TaskSinkError::PushError(e))
            .send_all(&mut stream::iter(
                tasks
                    .into_iter()
                    .map(Task::new)
                    .map(|task| {
                        task.try_map(|t| {
                            C::encode(&t).map_err(|e| TaskSinkError::CodecError(e.into()))
                        })
                    })
                    .collect::<Vec<_>>(),
            ))
            .await
    }

    async fn push_stream(
        &mut self,
        tasks: impl Stream<Item = Args> + Unpin + Send,
    ) -> Result<(), TaskSinkError<Self::Error>> {
        use futures_util::SinkExt;
        use futures_util::StreamExt;
        self.sink_map_err(|e| TaskSinkError::PushError(e))
            .send_all(&mut tasks.map(Task::new).map(|task| {
                task.try_map(|t| C::encode(&t).map_err(|e| TaskSinkError::CodecError(e.into())))
            }))
            .await
    }

    async fn push_task(
        &mut self,
        task: Task<Args, Self::Context, Self::IdType>,
    ) -> Result<(), TaskSinkError<Self::Error>> {
        use futures_util::SinkExt;
        self.sink_map_err(|e| TaskSinkError::PushError(e))
            .send(task.try_map(|t| C::encode(&t).map_err(|e| TaskSinkError::CodecError(e.into())))?)
            .await
    }
}
