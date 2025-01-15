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
/// Test utilities that allows you to test backends
pub mod test_utils {
    use crate::backend::Backend;
    use crate::builder::WorkerBuilder;
    use crate::error::BoxDynError;
    use crate::request::Request;
    use crate::task::task_id::TaskId;
    use crate::worker::{Worker, WorkerId};
    use futures::channel::mpsc::{channel, Receiver, Sender};
    use futures::future::BoxFuture;
    use futures::stream::{Stream, StreamExt};
    use futures::{Future, FutureExt, SinkExt, TryFutureExt};
    use std::error::Error;
    use std::fmt::Debug;
    use std::marker::PhantomData;
    use std::ops::{Deref, DerefMut};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tower::layer::layer_fn;
    use tower::{Layer, Service, ServiceExt};

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
        backend: B,
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
    ///        let (_, resp) = tester.execute_next().await;
    ///        assert_eq!(resp, Ok("()".to_string()));
    ///    }
    ///}
    /// ````
    impl<B, Req: Sync, Res: Send + Debug + Sync + serde::Serialize + 'static, Ctx>
        TestWrapper<B, Request<Req, Ctx>, Res>
    where
        B: Backend<Request<Req, Ctx>, Res> + Send + Sync + 'static + Clone,
        Req: Send + 'static,
        Ctx: Send + Sync + 'static,
        B::Stream: Send + 'static,
        B::Stream: Stream<Item = Result<Option<Request<Req, Ctx>>, crate::error::Error>> + Unpin,
    {
        /// Build a new instance provided a custom service
        pub fn new_with_service<S>(backend: B, service: S) -> (Self, BoxFuture<'static, ()>)
        where
            S: Service<Request<Req, Ctx>, Response = Res> + Send + 'static + Sync,
            B::Layer: Layer<S>,
            <<B as Backend<Request<Req, Ctx>, Res>>::Layer as Layer<S>>::Service:
                Service<Request<Req, Ctx>, Response = Res> + Send + 'static,
            <<<B as Backend<Request<Req, Ctx>, Res>>::Layer as Layer<S>>::Service as Service<
                Request<Req, Ctx>,
            >>::Response: Send + Debug,
            <<<B as Backend<Request<Req, Ctx>, Res>>::Layer as Layer<S>>::Service as Service<
                Request<Req, Ctx>,
            >>::Error: Send + Into<BoxDynError> + Sync,
            <<<B as Backend<Request<Req, Ctx>, Res>>::Layer as Layer<S>>::Service as Service<
                Request<Req, Ctx>,
            >>::Future: Send + 'static,
            <S as Service<Request<Req, Ctx>>>::Future: Send,
            <S as Service<Request<Req, Ctx>>>::Error: Send,
            <S as Service<Request<Req, Ctx>>>::Error: Sync,
            <S as Service<Request<Req, Ctx>>>::Error: std::error::Error,
            <B as Backend<Request<Req, Ctx>, Res>>::Layer: Layer<TestEmitService<S>>,
            <<B as Backend<Request<Req, Ctx>, Res>>::Layer as Layer<TestEmitService<S>>>::Service:
                Service<Request<Req, Ctx>>,
        {
            use crate::builder::WorkerFactory;
            let (mut res_tx, res_rx) = channel(10);
            let worker = WorkerBuilder::new("test-worker")
                .layer(TestEmitLayer {
                    tx: res_tx,
                    service,
                })
                .backend(backend)
                .build(service);

            let (stop_tx, mut stop_rx) = channel::<()>(1);

            let poller = async move {
                worker.run().await;
            };
            (
                TestWrapper {
                    stop_tx,
                    res_rx,
                    _p: PhantomData,
                    backend,
                    _r: PhantomData,
                },
                poller.boxed(),
            )
        }

        /// Stop polling
        pub fn stop(mut self) {
            self.stop_tx.try_send(()).unwrap();
        }

        /// Gets the current state of results
        pub async fn execute_next(&mut self) -> (TaskId, Result<String, String>) {
            self.res_rx.next().await.unwrap()
        }
    }

    pub struct TestEmitService<S> {
        tx: Sender<(TaskId, Result<String, String>)>,
        service: S,
    }

    pub struct TestEmitLayer<S> {
        tx: Sender<(TaskId, Result<String, String>)>,
        service: S,
    }

    impl<S> Layer<S> for TestEmitLayer<S> {
        type Service = TestEmitService<S>;

        fn layer(&self, service: S) -> Self::Service {
            TestEmitService {
                tx: self.tx.clone(),
                service,
            }
        }
    }

    impl<S, Req, Ctx, Res: Debug + Send + Sync, Err: std::error::Error + Send>
        Service<Request<Req, Ctx>> for TestEmitService<S>
    where
        S: Service<Request<Req, Ctx>, Response = Res, Error = Err>,
        S::Future: Send + 'static,
    {
        type Response = S::Response;
        type Error = S::Error;
        type Future = Pin<Box<dyn Future<Output = Result<Res, Err>> + Send>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.service.poll_ready(cx)
        }

        fn call(&mut self, request: Request<Req, Ctx>) -> Self::Future {
            let task_id = request.parts.task_id.clone();
            let mut res_tx = self.tx.clone();
            let fut = self.service.call(request);
            Box::pin(async move {
                let res = fut.await;
                match &res {
                    Ok(res) => {
                        res_tx
                            .send((task_id, Ok(format!("{res:?}"))))
                            .await
                            .unwrap();
                    }
                    Err(err) => {
                        res_tx.send((task_id, Err(err.to_string()))).await.unwrap();
                    }
                }
                res
            })
        }
    }

    impl<B, Req, Res, Ctx> Deref for TestWrapper<B, Request<Req, Ctx>, Res>
    where
        B: Backend<Request<Req, Ctx>, Res>,
    {
        type Target = B;

        fn deref(&self) -> &Self::Target {
            &self.backend
        }
    }

    impl<B, Req, Ctx, Res> DerefMut for TestWrapper<B, Request<Req, Ctx>, Res>
    where
        B: Backend<Request<Req, Ctx>, Res>,
    {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.backend
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
                let _res = t.execute_next().await;
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
                assert_eq!(res, 0); // No jobs
                t.push(1).await.unwrap();
                let res = t.len().await.unwrap();
                assert_eq!(res, 1); // A job exists
                let res = t.execute_next().await;
                assert_eq!(res.1, Ok("1".to_owned()));
                // TODO: all storages need to satisfy this rule, redis does not
                // let res = t.len().await.unwrap();
                // assert_eq!(res, 0);
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
                assert_eq!(res, 0); // No jobs
                t.push(1).await.unwrap();
                let res = t.len().await.unwrap();
                assert_eq!(res, 1); // A job exists
                let res = t.execute_next().await;
                assert_eq!(res.1, Ok("1".to_owned()));
                t.vacuum().await.unwrap();
                let res = t.len().await.unwrap();
                assert_eq!(res, 0); // After vacuuming, there should be nothing
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
                let res = t.execute_next().await;
                assert_eq!(res.1, Err("FailedError: oh no!".to_owned()));

                tokio::time::sleep(Duration::from_secs(1)).await;
                let task = backend.fetch_by_id(&parts.task_id).await.unwrap().unwrap();
                assert_eq!(task.parts.attempt.current(), 1, "should have 1 attempt");

                let res = t.execute_next().await;
                assert_eq!(res.1, Err("FailedError: oh no!".to_owned()));

                let task = backend.fetch_by_id(&parts.task_id).await.unwrap().unwrap();
                assert_eq!(task.parts.attempt.current(), 2);

                let res = t.execute_next().await;
                assert_eq!(res.1, Err("FailedError: oh no!".to_owned()));

                let task = backend.fetch_by_id(&parts.task_id).await.unwrap().unwrap();
                assert_eq!(task.parts.attempt.current(), 3);

                let res = t.execute_next().await;
                assert_eq!(res.1, Err("FailedError: oh no!".to_owned()));

                let task = backend.fetch_by_id(&parts.task_id).await.unwrap().unwrap();
                assert_eq!(task.parts.attempt.current(), 4);

                let res = t.len().await.unwrap();
                assert_eq!(res, 1); // The job still exists and there is no duplicates
            }
        };
    }
}
