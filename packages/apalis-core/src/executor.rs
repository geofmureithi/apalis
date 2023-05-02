use futures::Future;

/// An Executor that is used to spawn futures
pub trait Executor: Clone {
    /// Spawns a new asynchronous task
    fn spawn(&self, future: impl Future<Output = ()> + Send + 'static);
}

/// An Executor that uses the tokio runtime
#[cfg(feature = "tokio-comp")]
#[derive(Clone, Debug)]
pub struct TokioExecutor;

#[cfg(feature = "tokio-comp")]
impl TokioExecutor {
    /// A new tokio executor
    pub fn new() -> Self {
        Self
    }
}

#[cfg(feature = "tokio-comp")]
impl Executor for TokioExecutor {
    fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        tokio::spawn(future);
    }
}

/// An Executor that uses the async-std runtime
#[derive(Clone, Debug)]
#[cfg(feature = "async-std-comp")]
pub struct AsyncStdExecutor;

#[cfg(feature = "async-std-comp")]
impl AsyncStdExecutor {
    /// A new async-std executor
    pub fn new() -> Self {
        Self
    }
}

#[cfg(feature = "async-std-comp")]
impl Executor for AsyncStdExecutor {
    fn spawn(&self, fut: impl Future<Output = ()> + Send + 'static) {
        async_std::task::spawn(async { fut.await });
    }
}