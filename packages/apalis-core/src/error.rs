use std::{
    error::Error as StdError,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use thiserror::Error;
use tower::Service;

use crate::worker::WorkerError;

/// Convenience type alias
pub type BoxDynError = Box<dyn StdError + 'static + Send + Sync>;

/// Represents a general error returned by a task or by internals of the platform
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum Error {
    /// An error occurred during execution.
    #[error("FailedError: {0}")]
    Failed(#[source] Arc<BoxDynError>),

    /// Execution was aborted
    #[error("AbortError: {0}")]
    Abort(#[source] Arc<BoxDynError>),

    #[doc(hidden)]
    /// Encountered an error during worker execution
    /// This should not be used inside a task function
    #[error("WorkerError: {0}")]
    WorkerError(WorkerError),

    /// Missing some data and yet it was requested during execution.
    /// This should not be used inside a task function
    #[error("MissingDataError: {0}")]
    MissingData(String),

    #[doc(hidden)]
    /// Encountered an error during service execution
    /// This should not be used inside a task function
    #[error("Encountered an error during service execution")]
    ServiceError(#[source] Arc<BoxDynError>),

    #[doc(hidden)]
    /// Encountered an error during service execution
    /// This should not be used inside a task function
    #[error("Encountered an error during streaming")]
    SourceError(#[source] Arc<BoxDynError>),
}

impl From<BoxDynError> for Error {
    fn from(err: BoxDynError) -> Self {
        if let Some(e) = err.downcast_ref::<Error>() {
            e.clone()
        } else {
            Error::Failed(Arc::new(err))
        }
    }
}

/// A Tower layer for handling and converting service errors into a custom `Error` type.
///
/// This layer wraps a service and intercepts any errors returned by the service.
/// It attempts to downcast the error into the custom `Error` enum. If the downcast
/// succeeds, it returns the downcasted `Error`. If the downcast fails, the original
/// error is wrapped in `Error::Failed`.
///
/// The service's error type must implement `Into<BoxDynError>`, allowing for flexible
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

impl<S> tower::layer::Layer<S> for ErrorHandlingLayer {
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
    S::Error: Into<BoxDynError>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx).map_err(|e| {
            let boxed_error: BoxDynError = e.into();
            boxed_error.into()
        })
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let fut = self.service.call(req);

        Box::pin(async move {
            fut.await.map_err(|e| {
                let boxed_error: BoxDynError = e.into();
                boxed_error.into()
            })
        })
    }
}
