use std::{
    error::Error as StdError, marker::PhantomData, pin::Pin, task::{Context, Poll}, time::Duration
};
use thiserror::Error;
use tower_service::Service;

/// Convenience type alias
pub type BoxDynError = Box<dyn StdError + 'static + Send + Sync>;
/// Execution should be aborted
/// This signifies that the task should not be retried
#[derive(Error, Debug)]
#[error("AbortError: {source}")]
pub struct AbortError {
    #[source]
    source: BoxDynError,
}
impl AbortError {
    /// Create a new abort error
    pub fn new<E: Into<BoxDynError>>(err: E) -> Self {
        AbortError { source: err.into() }
    }
}

/// Execution should be retried after a specific duration
/// This increases the attempts
#[derive(Error, Debug)]
#[error("RetryError: {source}")]
pub struct RetryAfterError {
    #[source]
    source: BoxDynError,
    duration: Duration,
}

impl RetryAfterError {
    /// Create a new retry after error
    pub fn new<E: Into<BoxDynError>>(err: E, duration: Duration) -> Self {
        RetryAfterError {
            source: err.into(),
            duration,
        }
    }

    /// Get the duration after which the task should be retried
    pub fn get_duration(&self) -> Duration {
        self.duration
    }
}

/// Execution should be deferred, will be retried instantly
#[derive(Error, Debug)]
#[error("DeferredError: {source}")]
pub struct DeferredError {
    #[source]
    source: BoxDynError,
}

/// Possible errors that can occur when running a worker.
#[derive(Error, Debug)]
pub enum WorkerError {
    /// An error occurred while consuming the task stream.
    #[error("Failed to consume task stream: {0}")]
    StreamError(BoxDynError),
    /// An error occurred in the worker's heartbeat.
    #[error("Heartbeat error: {0}")]
    HeartbeatError(BoxDynError),
    /// An error occurred while trying to change the state of the worker.
    #[error("Failed to handle the new state: {0}")]
    StateError(#[from] WorkerStateError),
    /// A worker that terminates when .stop was called
    #[error("Worker stopped and gracefully exited")]
    GracefulExit,
    /// A worker panicked and the panic was caught.
    #[error("Worker panicked: {0}")]
    PanicError(String),
    /// An error occurred while handling io
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

/// Errors related to worker state transitions
#[derive(Error, Debug)]
pub enum WorkerStateError {
    /// Worker not started
    #[error("Worker not started, did you forget to call worker.start()")]
    NotStarted,
    /// Worker already started
    #[error("Worker already started")]
    AlreadyStarted,
    /// Worker is not running
    #[error("Worker is not running")]
    NotRunning,
    /// Worker is not paused
    #[error("Worker is not paused")]
    NotPaused,
    /// Worker is shutting down
    #[error("Worker is shutting down")]
    ShuttingDown,
    /// Invalid state provided
    #[error("Worker provided with invalid state {0}")]
    InvalidState(String),
}

/// A Tower layer for handling and converting service errors into a `BoxDynError`.
///
/// The service's error type must implement `std::error::Error`, allowing for flexible
/// error handling, especially when dealing with trait objects or complex error chains.
#[derive(Clone, Debug)]
pub struct ErrorHandlingLayer {
    _p: PhantomData<()>,
}

impl ErrorHandlingLayer {
    /// Create a new ErrorHandlingLayer
    pub fn new() -> Self {
        Self { _p: PhantomData }
    }
}

impl Default for ErrorHandlingLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> tower_layer::Layer<S> for ErrorHandlingLayer {
    type Service = ErrorHandlingService<S>;

    fn layer(&self, service: S) -> Self::Service {
        ErrorHandlingService { service }
    }
}

/// The underlying service
#[derive(Clone, Debug)]
pub struct ErrorHandlingService<S> {
    service: S,
}

impl<S, Request> Service<Request> for ErrorHandlingService<S>
where
    S: Service<Request>,
    S::Error: std::error::Error + Send + Sync + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxDynError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx).map_err(|e| {
            let boxed_error: BoxDynError = e.into();
            boxed_error
        })
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let fut = self.service.call(req);

        Box::pin(async move {
            fut.await.map_err(|e| {
                let boxed_error: BoxDynError = e.into();
                boxed_error
            })
        })
    }
}
