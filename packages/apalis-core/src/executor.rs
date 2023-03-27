use futures::Future;
use tokio::task::JoinHandle;

/// An Executor that is used to spawn futures
pub trait Executor: Clone {
    /// The result of the executor spawn
    type JoinHandle;
    /// Spawns a new asynchronous task
    fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) -> Self::JoinHandle;
}

/// An Executor that uses the tokio runtime
#[derive(Clone, Debug)]
pub struct TokioExecutor;

impl Executor for TokioExecutor {
    type JoinHandle = JoinHandle<()>;
    fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) -> Self::JoinHandle {
        tokio::spawn(future)
    }
}
