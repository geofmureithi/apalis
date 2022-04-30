use http::Extensions;
use std::any::Any;

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
    pub fn new() -> Self {
        JobContext {
            data: Default::default(),
        }
    }

    /// Get a reference to a type previously inserted on this `JobContext`.
    ///
    /// # Example
    ///
    /// ```
    /// # use apalis_core::context::JobContext;
    /// let mut ctx = JobContext::new();
    /// assert!(ctx.data_opt::<i32>().is_none());
    /// ctx.insert(5i32);
    ///
    /// assert_eq!(ctx.data_opt::<i32>(), Some(&5i32));
    /// ```
    pub fn data_opt<D: Any + Send + Sync>(&self) -> Option<&D> {
        self.data.get()
    }

    /// Insert a type into this `JobContext`.
    ///
    /// Important for embedding data for a job.
    /// If a extension of this type already existed, it will be returned.
    ///
    /// # Example
    ///
    /// ```
    /// # use apalis_core::context::JobContext;
    /// let mut ctx = JobContext::new();
    /// assert!(ctx.insert(5i32).is_none());
    /// assert!(ctx.insert(4u8).is_none());
    /// assert_eq!(ctx.insert(9i32), Some(5i32));
    /// ```
    pub fn insert<D: Any + Send + Sync>(&mut self, data: D) -> Option<D> {
        self.data.insert(data)
    }
}
