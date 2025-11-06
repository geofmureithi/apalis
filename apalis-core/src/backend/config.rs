use crate::backend::{Backend, queue::Queue};

/// Extension trait for accessing queue configuration
pub trait ConfigExt: Backend {
    /// Get the queue configuration
    fn get_queue(&self) -> Queue;
}
