//! Provides a worker that allows testing and debugging
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
//! ```rust
//! # use apalis_core::{backend::memory::MemoryStorage, worker::test_worker::TestWorker};
//! # use apalis_core::error::BoxDynError;
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
//!     backend.push(42usize).await.unwrap();
//!     let mut worker = TestWorker::new(backend, is_even);
//!
//!     let (_task_id, resp) = worker.execute_next().await.unwrap().unwrap();
//!     assert_eq!(resp, Ok("()".to_string()));
//! }
//! ```
//!
//! This module is intended for use in tests and local development.

use crate::backend::Backend;
use crate::error::BoxDynError;
use crate::task::Task;
use crate::task::task_id::{RandomId, TaskId};
use crate::worker::builder::{IntoWorkerService, WorkerBuilder};
use crate::worker::{Event, ReadinessService, TrackerService, WorkerError};
use futures_channel::mpsc::{self, channel};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{SinkExt, StreamExt};
use std::fmt::{self, Debug};
use std::future::Future;
use std::marker::PhantomData;
use std::task::{Context, Poll};
use tower_layer::Layer;
use tower_service::Service;

type TestStream<IdType, Res> =
    BoxStream<'static, Result<(TaskId<IdType>, Result<Res, BoxDynError>), WorkerError>>;
/// A test worker to allow you to test services.
/// Important for testing backends and tasks
/// # Example
/// ```
/// mod tests {
///    use apalis_core::{error::BoxDynError, backend::memory::MemoryStorage};
///
///    use super::*;
///
///    async fn is_even(req: usize) -> Result<(), BoxDynError> {
///        if req % 2 == 0 {
///            Ok(())
///        } else {
///            Err("Not an even number".into())
///        }
///    }
///
///    #[tokio::test]
///    async fn test_accepts_even() {
///        let mut backend = MemoryStorage::new();
///        backend.push(42usize).await.unwrap();
///        let mut worker = TestWorker::new(backend, is_even);
///        let (_task_id, resp) = worker.execute_next().await.unwrap().unwrap();
///        assert_eq!(resp, Ok("()".to_string()));
///    }
///}
/// ````
pub struct TestWorker<B, S, Res, IdType = RandomId> {
    stream: TestStream<IdType, Res>,
    backend: PhantomData<B>,
    service: PhantomData<(S, Res)>,
}

impl<B, S, Res, IdType> fmt::Debug for TestWorker<B, S, Res, IdType> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestWorker")
            .field("stream", &"BoxStream<...>") // can't really debug streams
            .field("backend", &std::any::type_name::<B>())
            .field("service", &std::any::type_name::<(S, Res)>())
            .field("id_type", &std::any::type_name::<IdType>())
            .finish()
    }
}

/// Utility for executing the next item in the queue
pub trait ExecuteNext<Args, Ctx> {
    /// The expected result from the provided service
    type Result;
    /// Allows the test worker to step to the next task
    /// No polling is done in between calls
    fn execute_next(&mut self) -> impl Future<Output = Self::Result> + Send;
}

impl<B, S, Args, Ctx, Res, IdType> ExecuteNext<Args, Ctx> for TestWorker<B, S, Res, IdType>
where
    S: Service<Task<Args, Ctx, IdType>, Response = Res> + Send + 'static,
    B: Send,
    Res: Send,
{
    type Result = Option<Result<(TaskId<IdType>, Result<Res, BoxDynError>), WorkerError>>;
    async fn execute_next(&mut self) -> Self::Result {
        self.stream.next().await
    }
}

impl<B, S, Res> TestWorker<B, S, Res, ()> {
    /// Create a new test worker
    pub fn new<Args, Ctx, W>(backend: B, factory: W) -> TestWorker<W::Backend, S, Res, B::IdType>
    where
        W: IntoWorkerService<B, S, Args, Ctx>,
        W::Backend: Backend<
                Args = B::Args,
                Context = B::Context,
                IdType = B::IdType,
                Error = B::Error,
                Stream = B::Stream,
                Beat = B::Beat,
                Layer = B::Layer,
            > + 'static,
        B: Backend<Args = Args, Context = Ctx> + 'static,
        S: Service<Task<Args, Ctx, B::IdType>, Response = Res> + Send + 'static,
        B::Stream: Unpin + Send + 'static,
        B::Beat: Unpin + Send + 'static,
        Args: Send + 'static,
        Ctx: Send + 'static,
        B::Error: Into<BoxDynError> + Send + 'static,
        B::Layer: Layer<ReadinessService<TrackerService<TestEmitService<S, Res, B::IdType>>>>,
        S::Future: Send,
        S::Error: Into<BoxDynError> + Send + Sync,
        S::Response: Clone + Send,
        Res: 'static,
        <<B as Backend>::Layer as Layer<
            ReadinessService<TrackerService<TestEmitService<S, Res, B::IdType>>>,
        >>::Service: Service<Task<Args, Ctx, B::IdType>>,
        <<<B as Backend>::Layer as Layer<
            ReadinessService<TrackerService<TestEmitService<S, Res, B::IdType>>>,
        >>::Service as Service<Task<Args, Ctx, B::IdType>>>::Error: Into<BoxDynError> + Sync + Send,
        <<<B as Backend>::Layer as Layer<
            ReadinessService<TrackerService<TestEmitService<S, Res, B::IdType>>>,
        >>::Service as Service<Task<Args, Ctx, B::IdType>>>::Future: Send,
        <<B as Backend>::Layer as Layer<
            ReadinessService<TrackerService<TestEmitService<S, Res, B::IdType>>>,
        >>::Service: std::marker::Send + 'static,
        B::IdType: Send + Clone + 'static,
    {
        let worker_service = factory.into_service(backend);
        TestWorker::<W::Backend, S, Res, _>::new_with_svc(
            worker_service.backend,
            worker_service.service,
        )
    }
}

impl<B, S, Res> TestWorker<B, S, Res, ()> {
    /// Create a new test worker with a service
    pub fn new_with_svc<Args, Ctx>(backend: B, service: S) -> TestWorker<B, S, Res, B::IdType>
    where
        B: Backend<Args = Args, Context = Ctx> + 'static,
        S: Service<Task<Args, Ctx, B::IdType>, Response = Res> + Send + 'static,
        B::Stream: Unpin + Send + 'static,
        B::Beat: Unpin + Send + 'static,
        Args: Send + 'static,
        Ctx: Send + 'static,
        B::Error: Into<BoxDynError> + Send + 'static,
        B::Layer: Layer<ReadinessService<TrackerService<TestEmitService<S, Res, B::IdType>>>>,
        S::Future: Send,
        S::Error: Into<BoxDynError> + Send + Sync,
        S::Response: Clone + Send,
        Res: 'static,
        <<B as Backend>::Layer as Layer<
            ReadinessService<TrackerService<TestEmitService<S, Res, B::IdType>>>,
        >>::Service: Service<Task<Args, Ctx, B::IdType>>,
        <<<B as Backend>::Layer as Layer<
            ReadinessService<TrackerService<TestEmitService<S, Res, B::IdType>>>,
        >>::Service as Service<Task<Args, Ctx, B::IdType>>>::Error: Into<BoxDynError> + Sync + Send,
        <<<B as Backend>::Layer as Layer<
            ReadinessService<TrackerService<TestEmitService<S, Res, B::IdType>>>,
        >>::Service as Service<Task<Args, Ctx, B::IdType>>>::Future: Send,
        <<B as Backend>::Layer as Layer<
            ReadinessService<TrackerService<TestEmitService<S, Res, B::IdType>>>,
        >>::Service: std::marker::Send + 'static,
        B::IdType: Send + Clone + 'static,
    {
        enum Item<R, IdType> {
            Ev(Result<Event, WorkerError>),
            Res((TaskId<IdType>, Result<R, BoxDynError>)),
        }
        let (tx, rx) = channel(1);
        let sender = tx.clone();
        let service: TestEmitService<S, Res, B::IdType> = TestEmitService {
            service,
            tx: tx.clone(),
        };
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
        TestWorker {
            stream,
            service: PhantomData,
            backend: PhantomData,
        }
    }
}

/// A generic service that emits the result of a test
#[derive(Debug, Clone)]
pub struct TestEmitService<S, Response, IdType> {
    tx: mpsc::Sender<(TaskId<IdType>, Result<Response, BoxDynError>)>,
    service: S,
}

impl<S, Args, Ctx, Res, IdType> Service<Task<Args, Ctx, IdType>> for TestEmitService<S, Res, IdType>
where
    S: Service<Task<Args, Ctx, IdType>, Response = Res> + Send + 'static,
    S::Future: Send + 'static,
    Args: Send + 'static,
    Ctx: Send + 'static,
    S::Response: Send + 'static,
    S::Error: Into<BoxDynError> + Send,
    IdType: Send + 'static + Clone,
{
    type Response = ();
    type Error = String;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service
            .poll_ready(cx)
            .map_err(|e| e.into().to_string())
    }

    fn call(&mut self, task: Task<Args, Ctx, IdType>) -> Self::Future {
        let task_id = task.parts.task_id.clone().unwrap();
        let mut tx = Clone::clone(&self.tx);
        let fut = self.service.call(task);
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
        backend::{TaskSink, memory::MemoryStorage},
        error::BoxDynError,
        task_fn::task_fn,
        worker::{
            WorkerContext,
            test_worker::{ExecuteNext, TestWorker},
        },
    };
    use std::time::Duration;

    #[tokio::test]
    async fn basic_worker() {
        let mut backend = MemoryStorage::new();

        for i in 0..=10 {
            backend.push(i).await.unwrap();
        }

        let service = task_fn(|req: u32, w: WorkerContext| async move {
            if req == 10 {
                w.stop()?;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok::<_, BoxDynError>(req)
        });
        let mut worker = TestWorker::new(backend, service);
        while let Some(Ok((_, ret))) = worker.execute_next().await {
            ret.unwrap();
        }
        println!("Worker run successfully");
    }
}
