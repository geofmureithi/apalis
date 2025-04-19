#![crate_name = "apalis_core"]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub,
    bad_style,
    dead_code,
    improper_ctypes,
    non_shorthand_field_patterns,
    no_mangle_generic_items,
    overflowing_literals,
    path_statements,
    patterns_in_fns_without_body,
    unconditional_recursion,
    unused,
    unused_allocation,
    unused_comparisons,
    unused_parens,
    while_true
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! # apalis-core
//! Utilities for building job and message processing tools.
/// Represent utilities for creating worker instances.
pub mod builder;

/// Represents a task source eg Postgres or Redis
pub mod backend;
/// Includes all possible error types.
pub mod error;
/// Represents middleware offered through [`tower`]
pub mod layers;
/// Represents monitoring of running workers
pub mod monitor;
/// Represents the request to be processed.
pub mod request;
/// Represents different possible responses.
pub mod response;
/// Represents a service that is created from a function.
pub mod service_fn;
/// Represents ability to persist and consume jobs from storages.
pub mod storage;
/// Represents the utils for building workers.
pub mod worker;

/// Represents the utils needed to extend a task's context.
pub mod data;
/// Message queuing utilities
pub mod mq;
/// Allows async listening in a mpsc style.
pub mod notify;
/// Controlled polling and streaming
pub mod poller;

/// In-memory utilities for testing and mocking
pub mod memory;

/// Task management utilities
pub mod task;

/// Codec for handling data
pub mod codec;

/// Allows stepped tasks
pub mod step;

/// Sleep utilities
#[cfg(feature = "sleep")]
pub async fn sleep(duration: std::time::Duration) {
    futures_timer::Delay::new(duration).await;
}

#[cfg(feature = "sleep")]
/// Interval utilities
pub mod interval {
    use std::fmt;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use futures::future::BoxFuture;
    use futures::Stream;

    use crate::sleep;
    /// Creates a new stream that yields at a set interval.
    pub fn interval(duration: Duration) -> Interval {
        Interval {
            timer: Box::pin(sleep(duration)),
            interval: duration,
        }
    }

    /// A stream representing notifications at fixed interval
    #[must_use = "streams do nothing unless polled or .awaited"]
    pub struct Interval {
        timer: BoxFuture<'static, ()>,
        interval: Duration,
    }

    impl fmt::Debug for Interval {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Interval")
                .field("interval", &self.interval)
                .field("timer", &"a future represented `apalis_core::sleep`")
                .finish()
        }
    }

    impl Stream for Interval {
        type Item = ();

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match Pin::new(&mut self.timer).poll(cx) {
                Poll::Ready(_) => {}
                Poll::Pending => return Poll::Pending,
            };
            let interval = self.interval;
            let fut = std::mem::replace(&mut self.timer, Box::pin(sleep(interval)));
            drop(fut);
            Poll::Ready(Some(()))
        }
    }
}

#[cfg(feature = "test-utils")]
#[allow(unused)]
/// Test utilities that allows you to test backends
pub mod test_utils {
    use crate::backend::Backend;
    use crate::builder::{WorkerBuilder, WorkerFactory};
    use crate::request::Request;
    use crate::task::task_id::TaskId;
    use crate::worker::Worker;
    use futures::channel::mpsc::{self, channel, Receiver, Sender, TryRecvError};
    use futures::future::BoxFuture;
    use futures::stream::{Stream, StreamExt};
    use futures::{FutureExt, SinkExt};
    use std::fmt::Debug;
    use std::future::Future;
    use std::marker::PhantomData;
    use std::ops::{Deref, DerefMut};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use tower::{Layer, Service, ServiceBuilder};

    /// Define a dummy service
    #[derive(Debug, Clone)]
    pub struct DummyService;

    impl<Request: Send + 'static> Service<Request> for DummyService {
        type Response = Request;
        type Error = std::convert::Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: Request) -> Self::Future {
            let fut = async move { Ok(req) };
            Box::pin(fut)
        }
    }

    /// A generic backend wrapper that polls and executes jobs
    #[derive(Debug)]
    pub struct TestWrapper<B, Req, Res> {
        stop_tx: Sender<()>,
        res_rx: Receiver<(TaskId, Result<String, String>)>,
        _p: PhantomData<Req>,
        _r: PhantomData<Res>,
        should_next: Arc<AtomicBool>,
        /// The inner backend
        pub backend: B,
        /// The inner worker
        pub worker: Worker<crate::worker::Context>,
    }
    /// A test wrapper to allow you to test without requiring a worker.
    /// Important for testing backends and jobs
    /// # Example
    /// ```no_run
    /// #[cfg(tests)]
    /// mod tests {
    ///    use crate::{
    ///        error::Error, memory::MemoryStorage, mq::MessageQueue, service_fn::service_fn,
    ///    };
    ///
    ///    use super::*;
    ///
    ///    async fn is_even(req: usize) -> Result<(), Error> {
    ///        if req % 2 == 0 {
    ///            Ok(())
    ///        } else {
    ///            Err(Error::Abort("Not an even number".to_string()))
    ///        }
    ///    }
    ///
    ///    #[tokio::test]
    ///    async fn test_accepts_even() {
    ///        let backend = MemoryStorage::new();
    ///        let (mut tester, poller) = TestWrapper::new_with_service(backend, service_fn(is_even));
    ///        tokio::spawn(poller);
    ///        tester.enqueue(42usize).await.unwrap();
    ///        assert_eq!(tester.size().await.unwrap(), 1);
    ///        let (_, resp) = tester.execute_next().await.unwrap();
    ///        assert_eq!(resp, Ok("()".to_string()));
    ///    }
    ///}
    /// ````

    impl<B, Req, Res, Ctx> TestWrapper<B, Request<Req, Ctx>, Res>
    where
        B: Backend<Request<Req, Ctx>> + Send + Sync + 'static + Clone,
        Req: Send + 'static,
        Ctx: Send,
        B::Stream: Send + 'static,
        B::Stream: Stream<Item = Result<Option<Request<Req, Ctx>>, crate::error::Error>> + Unpin,
        Res: Debug,
    {
        /// Build a new instance provided a custom service
    pub fn new_with_service<Svc>(backend: B, service: Svc) -> (Self, BoxFuture<'static, ()>)
    where
        Svc::Future: Send,
        Svc: Send + Sync + Service<Request<Req, Ctx>, Response = Res> + 'static,
        Req: Send + Sync + 'static,
        Svc::Response: Send + Sync + 'static,
        Svc::Error: Send + Sync + std::error::Error,
        Ctx: Send + Sync + 'static,
        B: Backend<Request<Req, Ctx>> + 'static,
        B::Layer: Layer<Svc>,
        <B::Layer as Layer<Svc>>::Service: Service<Request<Req, Ctx>, Response = Res> + Send + Sync,
        B::Stream: Unpin + Send,
        Res: Send,
        <<B::Layer as Layer<Svc>>::Service as Service<Request<Req, Ctx>>>::Future: Send,
        <<B::Layer as Layer<Svc>>::Service as Service<Request<Req, Ctx>>>::Error:
            Send + Sync + std::error::Error,
        B::Layer: Layer<TestEmitService<Svc>>,
        <B::Layer as Layer<TestEmitService<Svc>>>::Service:
            Service<Request<Req, Ctx>, Response = Res> + Send + Sync,
        <<B::Layer as Layer<TestEmitService<Svc>>>::Service as Service<Request<Req, Ctx>>>::Future:
            Send,
        <<B::Layer as Layer<TestEmitService<Svc>>>::Service as Service<Request<Req, Ctx>>>::Error:
            Send + Sync + Sync + std::error::Error,
    {
            let (mut res_tx, res_rx) = channel(10);
            let should_next = Arc::new(AtomicBool::new(false));
            let service = ServiceBuilder::new()
                .layer_fn(|service| TestEmitService {
                    service,
                    tx: res_tx.clone(),
                    should_next: should_next.clone(),
                })
                .service(service);
            let worker = WorkerBuilder::new("test-worker")
                .backend(backend.clone())
                .build(service)
                .run();
            let handle = worker.get_handle();
            let (stop_tx, mut stop_rx) = channel::<()>(1);

            let poller = async move {
                let worker = worker.shared();
                loop {
                    futures::select! {
                        _ = stop_rx.next().fuse() => break,
                        _ = worker.clone().fuse() => {

                        },
                    }
                }
                res_tx.close_channel();
            };
            (
                TestWrapper {
                    stop_tx,
                    res_rx,
                    _p: PhantomData,
                    backend,
                    _r: PhantomData,
                    should_next,
                    worker: handle,
                },
                poller.boxed(),
            )
        }

        /// Stop polling
        pub fn stop(mut self) {
            self.stop_tx.try_send(()).unwrap();
        }

        /// Gets the current state of results
        pub async fn execute_next(&mut self) -> Option<(TaskId, Result<String, String>)> {
            self.should_next.store(true, Ordering::Release);
            self.res_rx.next().await
        }

        /// Gets the current state of results
        pub fn try_execute_next(
            &mut self,
        ) -> Result<Option<(TaskId, Result<String, String>)>, TryRecvError> {
            self.should_next.store(true, Ordering::Release);
            self.res_rx.try_next().map(|res| {
                self.should_next.store(false, Ordering::Release);
                res
            })
        }
    }

    impl<B, Req, Res, Ctx> Deref for TestWrapper<B, Request<Req, Ctx>, Res>
    where
        B: Backend<Request<Req, Ctx>>,
    {
        type Target = B;

        fn deref(&self) -> &Self::Target {
            &self.backend
        }
    }

    impl<B, Req, Ctx, Res> DerefMut for TestWrapper<B, Request<Req, Ctx>, Res>
    where
        B: Backend<Request<Req, Ctx>>,
    {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.backend
        }
    }

    /// A generic service that emits the result of a test
    #[derive(Debug, Clone)]
    pub struct TestEmitService<S> {
        tx: mpsc::Sender<(TaskId, Result<String, String>)>,
        service: S,
        should_next: Arc<AtomicBool>,
    }

    impl<S, Req, Ctx> Service<Request<Req, Ctx>> for TestEmitService<S>
    where
        S: Service<Request<Req, Ctx>> + Send + 'static,
        S::Future: Send + 'static,
        Req: Send + 'static,
        S::Response: Debug + Send + Sync,
        S::Error: std::error::Error + Send,
        Ctx: Send + 'static,
    {
        type Response = S::Response;
        type Error = S::Error;
        type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            if self.should_next.load(Ordering::Relaxed) {
                self.service.poll_ready(cx)
            } else {
                Poll::Pending
            }
        }

        fn call(&mut self, req: Request<Req, Ctx>) -> Self::Future {
            self.should_next.store(false, Ordering::Relaxed);
            let task_id = req.parts.task_id.clone();
            let mut tx = Clone::clone(&self.tx);
            let fut = self.service.call(req);
            Box::pin(async move {
                let res = fut.await;
                match &res {
                    Ok(res) => {
                        tx.send((task_id, Ok(format!("{res:?}")))).await.unwrap();
                    }
                    Err(err) => {
                        tx.send((task_id, Err(err.to_string()))).await.unwrap();
                    }
                }
                res
            })
        }
    }

    pub use tower::service_fn as apalis_test_service_fn;

    #[macro_export]
    /// Tests a generic mq
    macro_rules! test_message_queue {
        ($backend_instance:expr) => {
            #[tokio::test]
            async fn it_works_as_an_mq_backend() {
                let backend = $backend_instance;
                let service = apalis_test_service_fn(|request: Request<u32, ()>| async {
                    Ok::<_, io::Error>(request)
                });
                let (mut t, poller) = TestWrapper::new_with_service(backend, service);
                tokio::spawn(poller);
                t.enqueue(1).await.unwrap();
                tokio::time::sleep(Duration::from_secs(1)).await;
                let _res = t.execute_next().await.unwrap();
                // assert_eq!(res.len(), 1); // One job is done
            }
        };
    }
    #[macro_export]
    /// Tests a generic storage
    macro_rules! generic_storage_test {
        ($setup:path ) => {
            #[tokio::test]
            async fn integration_test_storage_push_and_consume() {
                let backend = $setup().await;
                let service = apalis_test_service_fn(|request: Request<u32, _>| async move {
                    Ok::<_, io::Error>(request.args)
                });
                let (mut t, poller) = TestWrapper::new_with_service(backend, service);
                tokio::spawn(poller);
                let res = t.len().await.unwrap();
                assert_eq!(res, 0, "There should be no jobs");
                t.push(1).await.unwrap();
                let res = t.len().await.unwrap();
                assert_eq!(res, 1, "There should be 1 job");
                let res = t.execute_next().await.unwrap();
                assert_eq!(res.1, Ok("1".to_owned()));

                apalis_core::sleep(Duration::from_secs(1)).await;
                let res = t.len().await.unwrap();
                assert_eq!(res, 0, "There should be no jobs");

                t.vacuum().await.unwrap();
            }
            #[tokio::test]
            async fn integration_test_storage_vacuum() {
                let backend = $setup().await;
                let service = apalis_test_service_fn(|request: Request<u32, _>| async move {
                    Ok::<_, io::Error>(request.args)
                });
                let (mut t, poller) = TestWrapper::new_with_service(backend, service);
                tokio::spawn(poller);
                let res = t.len().await.unwrap();
                assert_eq!(res, 0, "There should be no jobs");
                t.push(1).await.unwrap();
                let res = t.len().await.unwrap();
                assert_eq!(res, 1, "A job exists");
                let res = t.execute_next().await.unwrap();
                assert_eq!(res.1, Ok("1".to_owned()));
                apalis_core::sleep(Duration::from_secs(1)).await;
                let res = t.len().await.unwrap();
                assert_eq!(res, 0, "There should be no job");

                t.vacuum().await.unwrap();
                let res = t.len().await.unwrap();
                assert_eq!(res, 0, "After vacuuming, there should be nothing");
            }

            #[tokio::test]
            async fn integration_test_storage_retry_persists() {
                use std::io::{Error, ErrorKind};
                let mut backend = $setup().await;
                let service = apalis_test_service_fn(|request: Request<u32, _>| async move {
                    Err::<String, io::Error>(Error::new(ErrorKind::Other, "oh no!"))
                });
                let (mut t, poller) = TestWrapper::new_with_service(backend.clone(), service);
                tokio::spawn(poller);
                let res = t.len().await.unwrap();
                assert_eq!(res, 0, "should have no jobs"); // No jobs
                let parts = t.push(1).await.unwrap();
                let res = t.len().await.unwrap();
                assert_eq!(res, 1, "should have 1 job"); // A job exists
                let res = t.execute_next().await.unwrap();
                assert_eq!(res.1, Err("oh no!".to_owned()));

                apalis_core::sleep(Duration::from_secs(1)).await;

                let task = backend.fetch_by_id(&parts.task_id).await.unwrap().unwrap();
                assert_eq!(task.parts.attempt.current(), 1, "should have 1 attempt");

                let res = t
                    .execute_next()
                    .await
                    .expect("Job must be added back to the queue after attempt 1");
                assert_eq!(res.1, Err("oh no!".to_owned()));

                apalis_core::sleep(Duration::from_secs(1)).await;
                let task = backend.fetch_by_id(&parts.task_id).await.unwrap().unwrap();
                assert_eq!(task.parts.attempt.current(), 2, "should have 2 attempts");

                let res = t
                    .execute_next()
                    .await
                    .expect("Job must be added back to the queue after attempt 2");
                assert_eq!(res.1, Err("oh no!".to_owned()));

                apalis_core::sleep(Duration::from_secs(1)).await;
                let task = backend.fetch_by_id(&parts.task_id).await.unwrap().unwrap();
                assert_eq!(task.parts.attempt.current(), 3, "should have 3 attempts");

                let res = t
                    .execute_next()
                    .await
                    .expect("Job must be added back to the queue after attempt 3");
                assert_eq!(res.1, Err("oh no!".to_owned()));
                apalis_core::sleep(Duration::from_secs(1)).await;

                let task = backend.fetch_by_id(&parts.task_id).await.unwrap().unwrap();
                assert_eq!(task.parts.attempt.current(), 4, "should have 4 attempts");

                let res = t
                    .execute_next()
                    .await
                    .expect("Job must be added back to the queue after attempt 5");
                assert_eq!(res.1, Err("oh no!".to_owned()));
                apalis_core::sleep(Duration::from_secs(1)).await;

                let task = backend.fetch_by_id(&parts.task_id).await.unwrap().unwrap();
                assert_eq!(task.parts.attempt.current(), 5, "should have 5 attempts");

                apalis_core::sleep(Duration::from_secs(1)).await;

                let res = t.len().await.unwrap();
                // Integration tests should include a max of 5 retries after that job should be aborted
                assert_eq!(res, 0, "should have no job");

                let res = t.try_execute_next();
                assert!(res.is_err());

                let task = backend.fetch_by_id(&parts.task_id).await.unwrap().unwrap();
                assert_eq!(
                    task.parts.attempt.current(),
                    5,
                    "should still have 5 attempts"
                );

                t.vacuum().await.unwrap();
                let res = t.len().await.unwrap();
                assert_eq!(res, 0, "After vacuuming, there should be nothing");
            }

            #[tokio::test]
            async fn integration_test_storage_abort() {
                let backend = $setup().await;
                let service = apalis_test_service_fn(|request: Request<u32, _>| async move {
                    Err::<(), _>(Error::Abort(std::sync::Arc::new(Box::new(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "request was invalid",
                    )))))
                });
                let (mut t, poller) = TestWrapper::new_with_service(backend, service);
                tokio::spawn(poller);
                let res = t.len().await.unwrap();
                assert_eq!(res, 0); // No jobs
                t.push(1).await.unwrap();
                let res = t.len().await.unwrap();
                assert_eq!(res, 1); // A job exists
                let res = t.execute_next().await.unwrap();
                assert_eq!(res.1, Err("AbortError: request was invalid".to_owned()));
                // Allow lazy storages to sync
                apalis_core::sleep(Duration::from_secs(1)).await;
                // Rechecking the queue len should return 0
                let res = t.len().await.unwrap();
                assert_eq!(
                    res, 0,
                    "The queue should be empty as the previous task was aborted"
                );

                t.vacuum().await.unwrap();
                let res = t.len().await.unwrap();
                assert_eq!(res, 0, "After vacuuming, there should be nothing");
            }

            #[tokio::test]
            async fn integration_test_storage_unexpected_abort() {
                let backend = $setup().await;
                let service = apalis_test_service_fn(|request: Request<u32, _>| async move {
                    None::<()>.unwrap(); // unexpected abort
                    Ok::<_, io::Error>(request.args)
                });
                let (mut t, poller) = TestWrapper::new_with_service(backend, service);
                tokio::spawn(poller);
                let res = t.len().await.unwrap();
                assert_eq!(res, 0, "There should be no jobs");
                let parts = t.push(1).await.unwrap();
                let res = t.len().await.unwrap();
                assert_eq!(res, 1, "A job exists");
                let res = t.execute_next().await;
                assert_eq!(res, None, "Our worker is dead");

                let res = t.len().await.unwrap();
                assert_eq!(res, 0, "Job should not have been re-added to the queue");

                // We start a healthy worker
                let service = apalis_test_service_fn(|request: Request<u32, _>| async move {
                    Ok::<_, io::Error>(request.args)
                });
                let (mut t, poller) = TestWrapper::new_with_service(t.backend, service);
                tokio::spawn(poller);

                apalis_core::sleep(Duration::from_secs(1)).await;
                // This is testing resuming the same worker
                // This ensures that the worker resumed any jobs lost during an interruption
                let res = t
                    .execute_next()
                    .await
                    .expect("Task must have been recovered and added to the queue");
                assert_eq!(res.1, Ok("1".to_owned()));

                let res = t.len().await.unwrap();
                assert_eq!(res, 0, "Task should have been consumed");

                t.vacuum().await.unwrap();
                let res = t.len().await.unwrap();
                assert_eq!(res, 0, "After vacuuming, there should be nothing");
            }
        };
    }
}
