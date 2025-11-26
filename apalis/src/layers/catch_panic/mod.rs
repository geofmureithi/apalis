//! # Middleware for catching panics
//!
//! The [`CatchPanicLayer`] allows you to catch panics in your task handlers and convert them into errors. This helps prevent panics from crashing your worker process and enables custom error handling or reporting.
//!
//! ## Usage
//!
//! ### Basic Worker Example
//!
//! ```rust
//! # use apalis::prelude::*;
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut in_memory = MemoryStorage::new();
//!     in_memory.push(42).await.unwrap();
//!
//!     async fn task(task: u32) {
//!         if task == 42 {
//!             panic!("I am a panic, catch me!");
//!         }
//!     }
//!
//!     let worker = WorkerBuilder::new("rango-tango")
//!         .backend(in_memory)
//!         .catch_panic()
//!         .on_event(|ctx, ev| {
//!             if matches!(ev, Event::Error(_)) {
//!                 ctx.stop().unwrap();
//!             }
//!         })
//!         .build(task);
//!     worker.run().await.unwrap();
//! }
//! ```
//!
//! ### Worker With Custom Panic Handler
//!
//! ```rust
//! # use apalis::layers::catch_panic::CatchPanicLayer;
//! # use apalis::layers::catch_panic::PanicError;
//! # use apalis::layers::retry::RetryPolicy;
//! # use apalis::prelude::*;
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut in_memory = MemoryStorage::new();
//!     in_memory.push(42).await.unwrap();
//!
//!     async fn task(task: u32) {
//!         if task == 42 {
//!             panic!("I am a panic, catch me!");
//!         }
//!     }
//!
//!     let worker = WorkerBuilder::new("rango-tango")
//!         .backend(in_memory)
//!         .retry(
//!             RetryPolicy::retries(1)
//!                 // Do not retry panics
//!                 .retry_if(|e: &BoxDynError| e.downcast_ref::<PanicError>().is_none()),
//!         )
//!         .layer(CatchPanicLayer::with_panic_handler(|e| {
//!             println!("Caught panic: {:?}", e);
//!             PanicError("Custom panic handler".to_string())
//!         }))
//!         .on_event(|ctx, ev| {
//!             if matches!(ev, Event::Error(_)) {
//!                 ctx.stop().unwrap();
//!             }
//!         })
//!         .build(task);
//!     worker.run().await.unwrap();
//! }
//! ```
use apalis_core::error::{AbortError, BoxDynError};
use apalis_core::task::Task;
use futures_util::FutureExt;
use futures_util::future::CatchUnwind;
use std::any::Any;
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::task::ready;
use std::task::{Context, Poll};
use tower::Layer;
use tower::Service;

/// Apalis Layer that catches panics in the service.
#[derive(Clone, Debug)]
pub struct CatchPanicLayer<F, Err> {
    on_panic: F,
    _marker: std::marker::PhantomData<Err>,
}

impl<Err> CatchPanicLayer<fn(Box<dyn Any + Send + 'static>) -> AbortError, Err> {
    /// Creates a new `CatchPanicLayer` with a default panic handler.
    pub fn new() -> Self {
        CatchPanicLayer {
            on_panic: default_handler,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Err> Default for CatchPanicLayer<fn(Box<dyn Any + Send>) -> AbortError, Err> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F, Err> CatchPanicLayer<F, Err>
where
    F: FnMut(Box<dyn Any + Send>) -> Err + Clone,
{
    /// Creates a new `CatchPanicLayer` with a custom panic handler.
    pub fn with_panic_handler(on_panic: F) -> Self {
        CatchPanicLayer {
            on_panic,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S, F, Err> Layer<S> for CatchPanicLayer<F, Err>
where
    F: FnMut(Box<dyn Any + Send>) -> Err + Clone,
{
    type Service = CatchPanicService<S, F>;

    fn layer(&self, service: S) -> Self::Service {
        CatchPanicService {
            service,
            on_panic: self.on_panic.clone(),
        }
    }
}

/// Apalis Service that catches panics.
#[derive(Clone, Debug)]
pub struct CatchPanicService<S, F> {
    service: S,
    on_panic: F,
}

impl<S, Req, Res, Ctx, F, PanicErr, IdType> Service<Task<Req, Ctx, IdType>>
    for CatchPanicService<S, F>
where
    S: Service<Task<Req, Ctx, IdType>, Response = Res>,
    F: FnMut(Box<dyn Any + Send>) -> PanicErr + Clone,
    S::Error: Into<BoxDynError>,
    PanicErr: Into<BoxDynError>,
{
    type Response = S::Response;
    type Error = BoxDynError;
    type Future = CatchPanicFuture<S::Future, F, PanicErr>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, task: Task<Req, Ctx, IdType>) -> Self::Future {
        match std::panic::catch_unwind(AssertUnwindSafe(|| self.service.call(task))) {
            Ok(future) => CatchPanicFuture {
                kind: Kind::Future {
                    future: AssertUnwindSafe(future).catch_unwind(),
                    panic_handler: Some(self.on_panic.clone()),
                },
                _marker: std::marker::PhantomData,
            },
            Err(panic_err) => CatchPanicFuture {
                kind: Kind::Panicked {
                    panic_err: Some(panic_err),
                    panic_handler: Some(self.on_panic.clone()),
                },
                _marker: std::marker::PhantomData,
            },
        }
    }
}

#[pin_project::pin_project(project = KindProj)]
enum Kind<F, T> {
    Panicked {
        panic_err: Option<Box<dyn Any + Send + 'static>>,
        panic_handler: Option<T>,
    },
    Future {
        #[pin]
        future: CatchUnwind<AssertUnwindSafe<F>>,
        panic_handler: Option<T>,
    },
}

/// A wrapper that catches panics during execution
#[pin_project::pin_project]
pub struct CatchPanicFuture<Fut, F, Err> {
    #[pin]
    kind: Kind<Fut, F>,
    _marker: std::marker::PhantomData<Err>,
}

impl<Fut, F, Err> fmt::Debug for CatchPanicFuture<Fut, F, Err> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatchPanicFuture")
            .field("kind", &"<hidden>")
            .field("_marker", &std::any::type_name::<Err>())
            .finish()
    }
}

/// An error generated from a panic
#[derive(Debug, Clone)]
pub struct PanicError(pub String);

impl std::error::Error for PanicError {}

impl fmt::Display for PanicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PanicError: {}", self.0)
    }
}

impl<Fut, Res, F, Err, PanicErr> Future for CatchPanicFuture<Fut, F, PanicErr>
where
    Fut: Future<Output = Result<Res, Err>>,
    F: FnMut(Box<dyn Any + Send>) -> PanicErr,
    Err: Into<BoxDynError>,
    PanicErr: Into<BoxDynError>,
{
    type Output = Result<Res, BoxDynError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().kind.project() {
            KindProj::Panicked {
                panic_err,
                panic_handler,
            } => {
                let mut panic_handler = panic_handler
                    .take()
                    .expect("future polled after completion");
                let panic_err = panic_err.take().expect("future polled after completion");
                Poll::Ready(Err(panic_handler(panic_err).into()))
            }
            KindProj::Future {
                future,
                panic_handler,
            } => match ready!(future.poll(cx)) {
                Ok(Ok(res)) => Poll::Ready(Ok(res)),
                Ok(Err(svc_err)) => Poll::Ready(Err(svc_err.into())),
                Err(panic_err) => {
                    let mut panic_handler = panic_handler
                        .take()
                        .expect("future polled after completion");
                    Poll::Ready(Err(panic_handler(panic_err).into()))
                }
            },
        }
    }
}

fn default_handler(e: Box<dyn Any + Send>) -> AbortError {
    let panic_info = if let Some(s) = e.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = e.downcast_ref::<String>() {
        s.clone()
    } else {
        "Unknown panic".to_string()
    };
    // apalis assumes service functions are pure
    // therefore a panic should ideally abort
    AbortError::new(PanicError(panic_info))
}

#[cfg(test)]
mod tests {
    use crate::layers::WorkerBuilderExt;

    use super::*;

    use crate::layers::retry::RetryPolicy;
    use apalis_core::{
        backend::{TaskSink, memory::MemoryStorage},
        error::BoxDynError,
        task::task_id::RandomId,
        worker::{builder::WorkerBuilder, event::Event, ext::event_listener::EventListenerExt},
    };
    use std::task::{Context, Poll};
    use tower::Service;

    #[derive(Clone, Debug)]
    struct TestJob;

    #[derive(Clone)]
    struct TestService;

    impl Service<Task<TestJob, (), RandomId>> for TestService {
        type Response = usize;
        type Error = AbortError;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Task<TestJob, (), RandomId>) -> Self::Future {
            Box::pin(async { Ok(42) })
        }
    }

    #[tokio::test]
    async fn test_catch_panic_layer() {
        let layer = CatchPanicLayer::new();
        let mut service = layer.layer(TestService);

        let request = Task::new(TestJob);
        let response = service.call(request).await;

        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_catch_panic_layer_panics() {
        struct PanicService;

        impl Service<Task<TestJob, (), RandomId>> for PanicService {
            type Response = usize;
            type Error = AbortError;
            type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

            fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, _req: Task<TestJob, (), RandomId>) -> Self::Future {
                Box::pin(async {
                    None::<()>.unwrap();
                    todo!()
                })
            }
        }

        let layer = CatchPanicLayer::new();
        let mut service = layer.layer(PanicService);

        let request = Task::new(TestJob);
        let response = service.call(request).await;

        assert!(response.is_err());

        assert_eq!(
            response.unwrap_err().to_string(),
            *"AbortError: PanicError: called `Option::unwrap()` on a `None` value"
        );
    }

    #[tokio::test]
    async fn basic_worker_catch_panic() {
        let mut in_memory = MemoryStorage::new();
        in_memory.push(42).await.unwrap();

        async fn task(task: u32) {
            if task == 42 {
                panic!("I am a panic, catch me!");
            }
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .catch_panic()
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {ev:?}", ctx.name());
                if matches!(ev, Event::Error(_)) {
                    ctx.stop().unwrap();
                }
            })
            .build(task);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn custom_worker_catch_panic() {
        let mut in_memory = MemoryStorage::new();
        in_memory.push(42).await.unwrap();

        async fn task(task: u32) {
            if task == 42 {
                panic!("I am a panic, catch me!");
            }
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .retry(
                RetryPolicy::retries(1)
                    // Do not retry panics
                    .retry_if(|e: &BoxDynError| e.downcast_ref::<PanicError>().is_none()),
            )
            .layer(CatchPanicLayer::with_panic_handler(|e| {
                println!("Caught panic: {e:?}");
                PanicError("Custom panic handler".to_string())
            }))
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {ev:?}", ctx.name());
                if matches!(ev, Event::Error(_)) {
                    ctx.stop().unwrap();
                }
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
