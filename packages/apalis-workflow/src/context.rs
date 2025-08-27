use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use apalis_core::{
    backend::{codec::Codec, TaskResult, TaskSink, WaitForCompletion}, error::BoxDynError, task::{builder::TaskBuilder, metadata::MetadataExt, task_id::TaskId, Task}
};
use futures::{lock::Mutex, StreamExt};

use crate::{StepError, WorkflowRequest};

#[derive(Debug)]
pub struct StepContext<FlowSink, Encode> {
    pub current_step: usize,
    sink: FlowSink,
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

    pub async fn wait_for<O, Compact>(
        &self,
        task_ids: &Vec<TaskId<FlowSink::IdType>>,
    ) -> Result<Vec<TaskResult<O>>, FlowSink::Error>
    where
        O: Sync + Send,
        FlowSink: Sync + WaitForCompletion<O, Compact> + TaskSink<Compact>,
        FlowSink::IdType: Clone,
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

    pub async fn push_raw<Args, Compact>(
        &mut self,
        task: Task<Args, FlowSink::Meta, FlowSink::IdType>,
    ) -> Result<(), FlowSink::Error>
    where
        FlowSink: Sync + TaskSink<Compact>,
        Args: Send,
        FlowSink::Meta: Send + Default + MetadataExt<WorkflowRequest>,
        FlowSink::Error: Debug,
        // Meta::Error: Debug,
        FlowSink::IdType: Default + Clone,
        Encode: Codec<Args, Compact = Compact>,
        Encode::Error: Debug, // Meta::Error: Debug,
    {
        self.sink
            .push_raw(task.try_map(|s| Encode::encode(&s)).unwrap())
            .await
    }

    pub async fn push_step_with_index<T, Compact>(
        &mut self,
        index: usize,
        step: &T,
    ) -> Result<TaskId<FlowSink::IdType>, StepError<Encode::Error>>
    where
        FlowSink: Sync + TaskSink<Compact>,
        T: Send + Sync,
        FlowSink::Meta: Send + Default + MetadataExt<WorkflowRequest>,
        FlowSink::Error: Into<BoxDynError>,
        FlowSink::IdType: Default + Clone,
        Encode: Codec<T, Compact = Compact>,
    {
        let task_id = TaskId::new(FlowSink::IdType::default());
        let mut meta = FlowSink::Meta::default();
        meta.inject(WorkflowRequest { step_index: index });
        let task = TaskBuilder::new_with_metadata(
            Encode::encode(step).map_err(|e| StepError::CodecError(e))?,
            meta,
        )
        .with_task_id(task_id.clone())
        .build();
        self.sink
            .push_raw(task)
            .await
            .map_err(|e| StepError::SinkError(e.into()))?;
        Ok(task_id)
    }

    pub async fn push_step<T, Compact>(
        &mut self,
        step: &T,
    ) -> Result<TaskId<FlowSink::IdType>, StepError<Encode::Error>>
    where
        FlowSink: Sync + TaskSink<Compact>,
        T: Send + Sync,
        FlowSink::Meta: Send + Default + MetadataExt<WorkflowRequest>,
        FlowSink::Error: Into<BoxDynError>,
        //     // Meta::Error: Debug,
        FlowSink::IdType: Default + Clone,
        Encode: Codec<T, Compact = Compact>,
    {
        self.push_step_with_index(1, step).await
    }
}
