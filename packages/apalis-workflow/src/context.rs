use std::{fmt::Debug, marker::PhantomData};

use apalis_core::{
    backend::{codec::Codec, TaskResult, TaskSink, WaitForCompletion},
    error::BoxDynError,
    task::{builder::TaskBuilder, metadata::MetadataExt, task_id::TaskId, Task},
};
use futures::StreamExt;

use crate::{WorkflowError, WorkflowRequest};

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

    pub async fn push_compact_task<Compact>(
        &mut self,
        task: Task<Compact, FlowSink::Meta, FlowSink::IdType>,
    ) -> Result<(), WorkflowError>
    where
        FlowSink: Sync + TaskSink<Compact>,
        Compact: Send,
        FlowSink::Meta: Send + Default + MetadataExt<WorkflowRequest>,
        FlowSink::Error: Into<BoxDynError>,
        FlowSink::IdType: Default,
    {
        self.sink
            .push_task(task)
            .await
            .map_err(|e| WorkflowError::SinkError(e.into()))
    }

    pub async fn push_task<Args, Compact>(
        &mut self,
        task: Task<Args, FlowSink::Meta, FlowSink::IdType>,
    ) -> Result<(), WorkflowError>
    where
        FlowSink: Sync + TaskSink<Compact>,
        Args: Send,
        FlowSink::Meta: Send + Default + MetadataExt<WorkflowRequest>,
        FlowSink::Error: Into<BoxDynError>,
        FlowSink::IdType: Default,
        Encode: Codec<Args, Compact = Compact>,
        Encode::Error: Into<BoxDynError>,
    {
        self.sink
            .push_task(
                task.try_map(|s| Encode::encode(&s))
                    .map_err(|e| WorkflowError::CodecError(e.into()))?,
            )
            .await
            .map_err(|e| WorkflowError::SinkError(e.into()))
    }

    pub async fn push_step_with_index<T, Compact>(
        &mut self,
        index: usize,
        step: &T,
    ) -> Result<TaskId<FlowSink::IdType>, WorkflowError>
    where
        FlowSink: Sync + TaskSink<Compact>,
        T: Send + Sync,
        FlowSink::Meta: Send + Default + MetadataExt<WorkflowRequest>,
        FlowSink::Error: Into<BoxDynError>,
        <FlowSink::Meta as MetadataExt<WorkflowRequest>>::Error: Into<BoxDynError>,
        FlowSink::IdType: Default,
        Encode: Codec<T, Compact = Compact>,
        Encode::Error: Into<BoxDynError>,
    {
        let task_id = TaskId::new(FlowSink::IdType::default());
        let mut meta = FlowSink::Meta::default();
        meta.inject(WorkflowRequest { step_index: index })
            .map_err(|e| WorkflowError::MetadataError(e.into()))?;
        let task = TaskBuilder::new_with_metadata(
            Encode::encode(step).map_err(|e| WorkflowError::CodecError(e.into()))?,
            meta,
        )
        .with_task_id(task_id.clone())
        .build();
        self.sink
            .push_task(task)
            .await
            .map_err(|e| WorkflowError::SinkError(e.into()))?;
        Ok(task_id)
    }

    pub async fn push_step<T, Compact>(
        &mut self,
        step: &T,
    ) -> Result<TaskId<FlowSink::IdType>, WorkflowError>
    where
        FlowSink: Sync + TaskSink<Compact>,
        T: Send + Sync,
        FlowSink::Meta: Send + Default + MetadataExt<WorkflowRequest>,
        FlowSink::Error: Into<BoxDynError>,
        FlowSink::IdType: Default,
        Encode: Codec<T, Compact = Compact>,
        <FlowSink::Meta as MetadataExt<WorkflowRequest>>::Error: Into<BoxDynError>,
        Encode::Error: Into<BoxDynError>,
    {
        self.push_step_with_index(self.current_step + 1, step).await
    }
}
