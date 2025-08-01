//! Provides a [`TestWorker`] implementation for testing task execution in isolation.
//!
//! This module enables comprehensive testing of task services and backends by simulating
//! a real worker’s lifecycle. It allows developers to push jobs to a backend, run them
//! through a test worker, and capture the results for assertion without needing full runtime orchestration.
//!
//! # Features
//! - Pluggable with any backend implementing [`Backend`].
//! - Supports service functions created using [`Service`].
//! - Captures task output through [`TestEmitService`] for validation.
//!
//! # Example
//! ```no_run
//! use crate::{memory::MemoryStorage, service_fn::service_fn, test_utils::TestWorker};
//!
//! async fn is_even(req: usize) -> Result<(), BoxDynError> {
//!     if req % 2 == 0 {
//!         Ok(())
//!     } else {
//!         Err("Not an even number".into())
//!     }
//! }
//!
//! #[tokio::test]
//! async fn test_accepts_even() {
//!     let backend = MemoryStorage::new();
//!     backend.enqueue(42usize).await.unwrap();
//!     let mut worker = TestWorker::new(backend, service_fn(is_even));
//!
//!     let (_task_id, resp) = worker.execute_next().await.unwrap().unwrap();
//!     assert_eq!(resp, Ok("()".to_string()));
//! }
//! ```
//!
//! # Types
//! - [`TestWorker`] — Runs a worker against an in-memory or mock backend for testing.
//! - [`TestEmitService`] — Wraps a service and emits responses to an async channel for inspection.
//! - [`ExecuteNext`] — Trait for advancing the test worker to process the next task.
//!
//! This module is intended for use in tests and local development.
//! [`Service`]: tower::Service
use crate::backend::{Backend, Reschedule};
use crate::error::BoxDynError;
use crate::request::task_id::TaskId;
use crate::request::Request;
use crate::worker::builder::WorkerBuilder;
use crate::worker::{Event, ReadinessService, TrackerService, Worker, WorkerError};
use futures_channel::mpsc::{self, channel, Receiver, Sender, TryRecvError};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{FutureExt, SinkExt, StreamExt};
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::util::BoxService;
use tower::{Layer, Service, ServiceBuilder};

/// A test worker to allow you to test services.
/// Important for testing backends and tasks
/// # Example
/// ```no_run
/// #[cfg(tests)]
/// mod tests {
///    use crate::{error:BoxDynError, memory::MemoryStorage, service_fn::service_fn};
///
///    use super::*;
///
///    async fn is_even(req: usize) -> Result<(), BoxDynError> {
///        if req % 2 == 0 {
///            Ok(())
///        } else {
///            Err("Not an even number"?)
///        }
///    }
///
///    #[tokio::test]
///    async fn test_accepts_even() {
///        let backend = MemoryStorage::new();
///        backend.enqueue(42usize).await.unwrap();
///        let mut worker = TestWorker::new(backend, service_fn(is_even));
///        let (_task_id, resp) = worker.execute_next().await.unwrap().unwrap();
///        assert_eq!(resp, Ok("()".to_string()));
///    }
///}
/// ````
pub struct TestWorker<B, S, Res> {
    stream: BoxStream<'static, Result<(TaskId, Result<Res, BoxDynError>), WorkerError>>,
    backend: PhantomData<B>,
    service: PhantomData<(S, Res)>,
}

/// Utility for executing the next item in the queue
pub trait ExecuteNext<Args, Ctx> {
    type Result;
    fn execute_next(&mut self) -> impl Future<Output = Self::Result> + Send;
}

impl<B, S, Args, Ctx, Res> ExecuteNext<Args, Ctx> for TestWorker<B, S, Res>
where
    S: Service<Request<Args, Ctx>, Response = Res> + Send + 'static,
    B: Send,
    Res: Send,
{
    type Result = Option<Result<(TaskId, Result<Res, BoxDynError>), WorkerError>>;
    async fn execute_next(&mut self) -> Self::Result {
        self.stream.next().await
    }
}

impl<B, S, Res> TestWorker<B, S, Res> {
    /// Build a new test worker
    pub fn new<Args, Ctx>(backend: B, service: S) -> Self
    where
        B: Backend<Args, Ctx> + 'static,
        S: Service<Request<Args, Ctx>, Response = Res> + Send + 'static,
        B::Stream: Unpin + Send + 'static,
        B::Beat: Unpin + Send + 'static,
        Args: Send + 'static,
        Ctx: Send + 'static,
        B::Error: Into<BoxDynError> + Send + 'static,
        B::Layer: Layer<ReadinessService<TrackerService<TestEmitService<S, Res>>>>,
        S::Future: Send,
        S::Error: Into<BoxDynError> + Send + Sync,
        S::Response: Clone + Send,
        Res: 'static,
        <<B as Backend<Args, Ctx>>::Layer as Layer<
            ReadinessService<TrackerService<TestEmitService<S, Res>>>,
        >>::Service: Service<Request<Args, Ctx>>,
        <<<B as Backend<Args, Ctx>>::Layer as Layer<
            ReadinessService<TrackerService<TestEmitService<S, Res>>>,
        >>::Service as Service<Request<Args, Ctx>>>::Error: Into<BoxDynError> + Sync + Send,
        <<<B as Backend<Args, Ctx>>::Layer as Layer<
            ReadinessService<TrackerService<TestEmitService<S, Res>>>,
        >>::Service as Service<Request<Args, Ctx>>>::Future: Send,
        <<B as Backend<Args, Ctx>>::Layer as Layer<
            ReadinessService<TrackerService<TestEmitService<S, Res>>>,
        >>::Service: std::marker::Send + 'static,
    {
        enum Item<R> {
            Ev(Result<Event, WorkerError>),
            Res((TaskId, Result<R, BoxDynError>)),
        }
        let (tx, rx) = channel(1);
        let sender = tx.clone();
        let service = ServiceBuilder::new()
            .layer_fn(|service| TestEmitService {
                service,
                tx: tx.clone(),
            })
            .service(service);
        let stream = WorkerBuilder::new("test-worker")
            .backend(backend)
            .build(service)
            .stream()
            .map(|r| Item::Ev(r));
        let task_stream = rx.map(|s| Item::Res(s));
        let stream = futures_util::stream::select(task_stream, stream)
            .filter_map(move |s| {
                let mut tx = sender.clone();
                async move {
                    match s {
                        Item::Ev(Err(e)) => {
                            tx.close().await.unwrap();
                            Some(Err(e))
                        }
                        Item::Ev(_) => None,
                        Item::Res(r) => Some(Ok(r)),
                    }
                }
            })
            .boxed();
        Self {
            stream,
            service: PhantomData,
            backend: PhantomData,
        }
    }
}

/// A generic service that emits the result of a test
#[derive(Debug, Clone)]
pub struct TestEmitService<S, Response> {
    tx: mpsc::Sender<(TaskId, Result<Response, BoxDynError>)>,
    service: S,
}

impl<S, Args, Ctx, Res> Service<Request<Args, Ctx>> for TestEmitService<S, Res>
where
    S: Service<Request<Args, Ctx>, Response = Res> + Send + 'static,
    S::Future: Send + 'static,
    Args: Send + 'static,
    Ctx: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Into<BoxDynError> + Send,
{
    type Response = ();
    type Error = String;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service
            .poll_ready(cx)
            .map_err(|e| e.into().to_string())
    }

    fn call(&mut self, req: Request<Args, Ctx>) -> Self::Future {
        let task_id = req.parts.task_id.clone();
        let mut tx = Clone::clone(&self.tx);
        let fut = self.service.call(req);
        Box::pin(async move {
            let res = fut.await;
            match res {
                Ok(res) => {
                    tx.send((task_id, Ok(res))).await.unwrap();
                    Ok(())
                }
                Err(err) => {
                    let e = err.into();
                    let e_str = e.to_string();
                    tx.send((task_id, Err(e))).await.unwrap();
                    Err(e_str)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        backend::{memory::MemoryStorage, Backend, TaskSink},
        error::BoxDynError,
        service_fn::service_fn,
        worker::{
            test_worker::{ExecuteNext, TestWorker},
            WorkerContext,
        },
    };
    use std::time::Duration;

    #[tokio::test]
    async fn it_works() {
        let mut backend = MemoryStorage::new();

        let mut sink = backend.sink();
        for i in 0..=10 {
            sink.push(i).await.unwrap();
        }

        let service = service_fn(|req: u32, w: WorkerContext| async move {
            if (req == 10) {
                w.stop()?;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok::<_, BoxDynError>(req)
        });
        let mut worker = TestWorker::new(backend, service);
        while let Some(Ok((_, ret))) = worker.execute_next().await {
            dbg!(ret);
        }
        println!("Worker run successfully");
    }
}
