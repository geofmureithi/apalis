use http::Extensions;
use std::any::{Any, TypeId};

/// The context for a job is represented here
/// Used to provide a context when a job is defined through the [Job] trait
#[derive(Debug)]
pub struct JobContext {
    data: Extensions,
}

impl Default for JobContext {
    fn default() -> Self {
        JobContext::new()
    }
}

impl JobContext {
    pub(crate) fn new() -> Self {
        JobContext {
            data: Default::default(),
        }
    }

    pub fn data_opt<D: Any + Send + Sync>(&self) -> Option<&D> {
        self.data.get()
    }
    pub(crate) fn insert<D: Any + Send + Sync>(&mut self, data: D) {
        self.data.insert(data);
    }
}
