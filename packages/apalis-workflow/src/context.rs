use std::{fmt::Debug, marker::PhantomData};

use apalis_core::{
    backend::{TaskResult, WaitForCompletion},
    task::task_id::TaskId,
};
use futures::StreamExt;

#[derive(Debug)]
pub struct StepContext<FlowSink, Encode> {
    pub current_step: usize,
    pub(crate) sink: FlowSink,
    _marker: PhantomData<Encode>,
}

impl<FlowSink: Clone, Encode> Clone for StepContext<FlowSink, Encode> {
    fn clone(&self) -> Self {
        StepContext {
            current_step: self.current_step,
            sink: self.sink.clone(),
            _marker: PhantomData,
        }
    }
}

impl<FlowSink, Encode> StepContext<FlowSink, Encode> {
    pub fn new(backend: FlowSink, current_step: usize) -> Self {
        Self {
            current_step,
            sink: backend,
            _marker: PhantomData,
        }
    }

    pub async fn wait_for<O>(
        &self,
        task_ids: &Vec<TaskId<FlowSink::IdType>>,
    ) -> Result<Vec<TaskResult<O>>, FlowSink::Error>
    where
        O: Sync + Send,
        FlowSink: Sync + WaitForCompletion<O>,
    {
        let items = self
            .sink
            .wait_for(task_ids.clone())
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        Ok(items)
    }

    //     pub async fn push_compact_task<Compact>(
    //         &mut self,
    //         task: Task<Compact, FlowSink::Context, FlowSink::IdType>,
    //     ) -> Result<(), WorkflowError>
    //     where
    //         FlowSink: Sync + TaskSink<Compact>,
    //         Compact: Send,
    //         FlowSink::Context: Send + Default + MetadataExt<WorkflowRequest>,
    //         FlowSink::Error: std::error::Error + Send + Sync + 'static,
    //         FlowSink::IdType: Default,
    //     {
    //         self.sink
    //             .push_task(task)
    //             .await
    //             .map_err(|e| WorkflowError::SinkError(e.into()))
    //     }

    //     pub async fn push_task<Args, Compact>(
    //         &mut self,
    //         task: Task<Args, FlowSink::Context, FlowSink::IdType>,
    //     ) -> Result<(), WorkflowError>
    //     where
    //         FlowSink: Sync + TaskSink<Compact>,
    //         Args: Send,
    //         FlowSink::Context: Send + Default + MetadataExt<WorkflowRequest>,
    //         FlowSink::Error: Into<BoxDynError>,
    //         FlowSink::IdType: Default,
    //         Encode: Codec<Args, Compact = Compact>,
    //         Encode::Error: Into<BoxDynError>,
    //     {
    //         todo!()
    //         // self.sink
    //         //     .push_task(
    //         //         task.try_map(|s| Encode::encode(&s))
    //         //             .map_err(|e| WorkflowError::CodecError(e.into()))?,
    //         //     )
    //         //     .await
    //         //     .map_err(|e| WorkflowError::SinkError(e.into()))
    //     }

    //     pub async fn push_step_with_index<T>(
    //         &mut self,
    //         index: usize,
    //         step: T,
    //     ) -> Result<TaskId<FlowSink::IdType>, WorkflowError>
    //     where
    //         FlowSink: Sync + WeakTaskSink<T>,
    //         T: Send + Sync,
    //         FlowSink::Context: Send + Default + MetadataExt<WorkflowRequest>,
    //         FlowSink::Error: std::error::Error + Send + Sync + 'static,
    //         <FlowSink::Context as MetadataExt<WorkflowRequest>>::Error: Into<BoxDynError>,
    //         FlowSink::IdType: Default,
    //         Encode: Codec<T>,
    //         Encode::Error: Into<BoxDynError>,
    //     {
    //         let task_id = TaskId::new(FlowSink::IdType::default());
    //         let task = TaskBuilder::new(step)
    //             .with_task_id(task_id.clone())
    //             .meta(WorkflowRequest { step_index: index })
    //             .build();
    //         self.sink
    //             .push_task(task)
    //             .await
    //             .map_err(|e| WorkflowError::SinkError(e.into()))?;
    //         Ok(task_id)
    //     }

    //     pub async fn push_next_step<Err>(
    //         &mut self,
    //         step: FlowSink::Compact,
    //     ) -> Result<TaskId<FlowSink::IdType>, WorkflowError>
    //     where
    //         FlowSink: Sync
    //             + Backend<Error = Err>
    //             + Sink<Task<FlowSink::Compact, FlowSink::Context, FlowSink::IdType>, Error = Err>
    //             + Unpin,
    //         FlowSink::Context: Send + Default + MetadataExt<WorkflowRequest>,
    //         FlowSink::IdType: Default,
    //         <FlowSink::Context as MetadataExt<WorkflowRequest>>::Error:
    //             std::error::Error + Send + Sync + 'static,
    //         Err: std::error::Error + Send + Sync + 'static,
    //     {
    //         use futures::SinkExt;
    //         let index = self.current_step + 1;
    //         let task_id = TaskId::new(FlowSink::IdType::default());
    //         let task = TaskBuilder::new(step)
    //             .with_task_id(task_id.clone())
    //             .meta(WorkflowRequest { step_index: index })
    //             .build();
    //         self.sink
    //             .send(task)
    //             .await
    //             .map_err(|e| WorkflowError::SinkError(e.into()))?;
    //         Ok(task_id)
    //     }
}
