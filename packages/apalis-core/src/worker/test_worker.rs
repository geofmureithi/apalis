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
//! [`Service`]: tower_service::Service
use crate::backend::Backend;
use crate::error::BoxDynError;
use crate::task::task_id::TaskId;
use crate::task::Task;
use crate::worker::builder::WorkerBuilder;
use crate::worker::{Event, ReadinessService, TrackerService, WorkerError};
use futures_channel::mpsc::{self, channel};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{FutureExt, SinkExt, StreamExt};
use std::fmt::{self, Debug};
use std::future::Future;
use std::marker::PhantomData;
use std::task::{Context, Poll};
use tower_layer::Layer;
use tower_service::Service;
use ulid::Ulid;

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

pub struct TestWorker<B, S, Res, IdType = Ulid> {
    stream: BoxStream<'static, Result<(TaskId<IdType>, Result<Res, BoxDynError>), WorkerError>>,
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
pub trait ExecuteNext<Args, Meta> {
    /// The expected result from the provided service
    type Result;
    /// Allows the test worker to step to the next task
    /// No polling is done in between calls 
    fn execute_next(&mut self) -> impl Future<Output = Self::Result> + Send;
}

impl<B, S, Args, Meta, Res, IdType> ExecuteNext<Args, Meta> for TestWorker<B, S, Res, IdType>
where
    S: Service<Task<Args, Meta, IdType>, Response = Res> + Send + 'static,
    B: Send,
    Res: Send,
{
    type Result = Option<Result<(TaskId<IdType>, Result<Res, BoxDynError>), WorkerError>>;
    async fn execute_next(&mut self) -> Self::Result {
        self.stream.next().await
    }
}

impl<B, S, Res> TestWorker<B, S, Res, ()> {
    /// Build a new test worker
    pub fn new<Args, Meta>(backend: B, service: S) -> TestWorker<B, S, Res, B::IdType>
    where
        B: Backend<Args, Meta> + 'static,
        S: Service<Task<Args, Meta, B::IdType>, Response = Res> + Send + 'static,
        B::Stream: Unpin + Send + 'static,
        B::Beat: Unpin + Send + 'static,
        Args: Send + 'static,
        Meta: Send + 'static,
        B::Error: Into<BoxDynError> + Send + 'static,
        B::Layer: Layer<ReadinessService<TrackerService<TestEmitService<S, Res, B::IdType>>>>,
        S::Future: Send,
        S::Error: Into<BoxDynError> + Send + Sync,
        S::Response: Clone + Send,
        Res: 'static,
        <<B as Backend<Args, Meta>>::Layer as Layer<
            ReadinessService<TrackerService<TestEmitService<S, Res, B::IdType>>>,
        >>::Service: Service<Task<Args, Meta, B::IdType>>,
        <<<B as Backend<Args, Meta>>::Layer as Layer<
            ReadinessService<TrackerService<TestEmitService<S, Res, B::IdType>>>,
        >>::Service as Service<Task<Args, Meta, B::IdType>>>::Error: Into<BoxDynError> + Sync + Send,
        <<<B as Backend<Args, Meta>>::Layer as Layer<
            ReadinessService<TrackerService<TestEmitService<S, Res, B::IdType>>>,
        >>::Service as Service<Task<Args, Meta, B::IdType>>>::Future: Send,
        <<B as Backend<Args, Meta>>::Layer as Layer<
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

impl<S, Args, Meta, Res, IdType> Service<Task<Args, Meta, IdType>> for TestEmitService<S, Res, IdType>
where
    S: Service<Task<Args, Meta, IdType>, Response = Res> + Send + 'static,
    S::Future: Send + 'static,
    Args: Send + 'static,
    Meta: Send + 'static,
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

    fn call(&mut self, req: Task<Args, Meta, IdType>) -> Self::Future {
        let task_id = req.ctx.task_id.clone().unwrap();
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

        for i in 0..=10 {
            backend.push(i).await.unwrap();
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
