pub mod ready;

use async_trait::async_trait;
use graceful_shutdown::Shutdown;
use std::fmt::Debug;

#[derive(Debug, Clone)]
pub struct WorkerRef(pub String);

#[async_trait]
pub trait Worker<Job>: Sized {
    type Service;

    type Source;

    type Error: std::error::Error + Send + Sync + 'static + Debug;

    async fn start(self, ctx: WorkerContext) -> Result<(), Self::Error>;
}

/// Stores the Workers context
pub struct WorkerContext {
    pub (crate) shutdown: Shutdown,
}

impl WorkerContext {
    /// Allows spawning of futures that will be gracefully shutdown by the worker
    pub fn spawn() {}

    /// Calling this function triggers shutting down the worker
    pub fn shutdown(&self) {}
}
