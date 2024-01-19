use futures::Future;

/// An Executor that is used to spawn futures
pub trait Executor {
    /// Spawns a new asynchronous task
    fn spawn(&self, future: impl Future<Output = ()> + Send + 'static);
}
