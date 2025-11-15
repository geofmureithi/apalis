use std::{collections::HashMap, time::Duration};

use apalis_core::{backend::BackendExt, task::task_id::TaskId};
use serde::{Deserialize, Serialize};

use crate::SteppedService;

/// Router for workflow steps
#[derive(Debug, Default)]
pub struct WorkflowRouter<Backend>
where
    Backend: BackendExt,
{
    pub(super) steps:
        HashMap<usize, SteppedService<Backend::Compact, Backend::Context, Backend::IdType>>,
}

impl<Backend> WorkflowRouter<Backend>
where
    Backend: BackendExt,
{
    /// Create a new workflow router
    #[must_use]
    pub fn new() -> Self {
        Self {
            steps: HashMap::new(),
        }
    }
}
/// Result information for workflow steps
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct StepResult<Res, IdType> {
    /// Result produced by the step
    pub result: Res,
    /// Optional ID of the next task to execute
    pub next_task_id: Option<TaskId<IdType>>,
}

/// Enum representing the possible transitions in a workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GoTo<T = ()> {
    /// Proceed to the next step with the given value
    Next(T),
    /// Delay the execution for the specified duration
    DelayFor(Duration, T),
    /// Break the workflow with the given value
    Break(T),
    /// Marks the workflow as done
    Done,
}
