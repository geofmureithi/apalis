use std::sync::{atomic::AtomicUsize, Arc};

/// A wrapper to keep count of the attempts tried by a task
#[derive(Debug, Clone)]
pub struct Attempt(Arc<AtomicUsize>);

impl Default for Attempt {
    fn default() -> Self {
        Self(Arc::new(AtomicUsize::new(0)))
    }
}

impl Attempt {
    /// Build a new tracker
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a tracker from an existing value
    pub fn new_with_value(value: usize) -> Self {
        Self(Arc::new(AtomicUsize::from(value)))
    }

    /// Get the current value
    pub fn current(&self) -> usize {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Increase the current value
    pub fn increment(&self) -> usize {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}
