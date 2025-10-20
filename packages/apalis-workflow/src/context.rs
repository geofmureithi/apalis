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
}
