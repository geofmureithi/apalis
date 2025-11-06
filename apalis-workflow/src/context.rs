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
    pub has_next: bool,
    _marker: PhantomData<Encode>,
}

impl<FlowSink: Clone, Encode> Clone for StepContext<FlowSink, Encode> {
    fn clone(&self) -> Self {
        Self {
            current_step: self.current_step,
            sink: self.sink.clone(),
            has_next: self.has_next,
            _marker: PhantomData,
        }
    }
}

impl<FlowSink, Encode> StepContext<FlowSink, Encode> {
    pub fn new(backend: FlowSink, current_step: usize, has_next: bool) -> Self {
        Self {
            current_step,
            sink: backend,
            _marker: PhantomData,
            has_next,
        }
    }

    pub async fn wait_for<O>(
        &self,
        task_ids: &[TaskId<FlowSink::IdType>],
    ) -> Result<Vec<TaskResult<O>>, FlowSink::Error>
    where
        O: Sync + Send,
        FlowSink: Sync + WaitForCompletion<O>,
        FlowSink::IdType: Clone,
    {
        let items = self
            .sink
            .wait_for(task_ids.to_vec())
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        Ok(items)
    }
}
