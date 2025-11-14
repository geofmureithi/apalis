use serde::{Deserialize, Serialize};

/// Context information for the current step in the workflow
#[derive(Debug, Clone)]
pub struct StepContext<Backend> {
    /// Index of the current step
    pub current_step: usize,
    /// Backend associated with the current step
    pub backend: Backend,
    /// Indicates if there is a next step
    pub has_next: bool,
}
impl<B> StepContext<B> {
    /// Creates a new StepContext
    pub fn new(backend: B, idx: usize, has_next: bool) -> Self {
        Self {
            current_step: idx,
            backend,
            has_next,
        }
    }
}

/// Metadata stored in each task for workflow processing
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct WorkflowContext {
    /// Index of the step in the workflow
    pub step_index: usize,
    // / Additional fields can be added as needed
    // / name: String,
    // / version: String,
    // / parent_workflow_id: Option<String>,
}
