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
use error::BoxDynError;
use futures::Stream;
use poller::Poller;
use serde::{Deserialize, Serialize};
use tower::Service;
use worker::WorkerId;

/// Represent utilities for creating worker instances.
pub mod builder;
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

/// A backend represents a task source
/// Both [`Storage`] and [`MessageQueue`] need to implement it for workers to be able to consume tasks
///
/// [`Storage`]: crate::storage::Storage
/// [`MessageQueue`]: crate::mq::MessageQueue
pub trait Backend<Req, Res> {
    /// The stream to be produced by the backend
    type Stream: Stream<Item = Result<Option<Req>, crate::error::Error>>;

    /// Returns the final decoration of layers
    type Layer;

    /// Returns a poller that is ready for streaming
    fn poll<Svc: Service<Req, Response = Res>>(
        self,
        worker: WorkerId,
    ) -> Poller<Self::Stream, Self::Layer>;
}
/// A codec allows backends to encode and decode data
pub trait Codec {
    /// The mode of storage by the codec
    type Compact;
    /// Error encountered by the codec
    type Error: Into<BoxDynError>;
    /// The encoding method
    fn encode<I>(input: I) -> Result<Self::Compact, Self::Error>
    where
        I: Serialize;
    /// The decoding method
    fn decode<O>(input: Self::Compact) -> Result<O, Self::Error>
    where
        O: for<'de> Deserialize<'de>;
}

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

#[cfg(test)]
#[doc(hidden)]
#[derive(Debug, Default, Clone)]
pub(crate) struct TestExecutor;
#[cfg(test)]
impl crate::executor::Executor for TestExecutor {
    fn spawn(&self, future: impl futures::prelude::Future<Output = ()> + Send + 'static) {
        tokio::spawn(future);
    }
}

#[cfg(feature = "test-utils")]
/// Test utilities that allows you to test backends
pub mod test_utils {
    use crate::error::BoxDynError;
    use crate::request::Request;
    use crate::task::task_id::TaskId;
    use crate::worker::WorkerId;
    use crate::Backend;
    use futures::channel::mpsc::{channel, Receiver, Sender};
    use futures::future::BoxFuture;
    use futures::stream::{Stream, StreamExt};
    use futures::{Future, FutureExt, SinkExt};
    use std::fmt::Debug;
    use std::marker::PhantomData;
    use std::ops::{Deref, DerefMut};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tower::{Layer, Service};

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
    impl<B, Req, Res, Ctx> TestWrapper<B, Request<Req, Ctx>, Res>
    where
        B: Backend<Request<Req, Ctx>, Res> + Send + Sync + 'static + Clone,
        Req: Send + 'static,
        Ctx: Send,
        B::Stream: Send + 'static,
        B::Stream: Stream<Item = Result<Option<Request<Req, Ctx>>, crate::error::Error>> + Unpin,
    {
        /// Build a new instance provided a custom service
        pub fn new_with_service<S>(backend: B, service: S) -> (Self, BoxFuture<'static, ()>)
        where
            S: Service<Request<Req, Ctx>, Response = Res> + Send + 'static,
            B::Layer: Layer<S>,
            <<B as Backend<Request<Req, Ctx>, Res>>::Layer as Layer<S>>::Service:
                Service<Request<Req, Ctx>> + Send + 'static,
            <<<B as Backend<Request<Req, Ctx>, Res>>::Layer as Layer<S>>::Service as Service<
                Request<Req, Ctx>,
            >>::Response: Send + Debug,
            <<<B as Backend<Request<Req, Ctx>, Res>>::Layer as Layer<S>>::Service as Service<
                Request<Req, Ctx>,
            >>::Error: Send + Into<BoxDynError> + Sync,
            <<<B as Backend<Request<Req, Ctx>, Res>>::Layer as Layer<S>>::Service as Service<
                Request<Req, Ctx>,
            >>::Future: Send + 'static,
        {
            let worker_id = WorkerId::new("test-worker");
            let b = backend.clone();
            let mut poller = b.poll::<S>(worker_id);
            let (stop_tx, mut stop_rx) = channel::<()>(1);

            let (mut res_tx, res_rx) = channel(10);

            let mut service = poller.layer.layer(service);

            let poller = async move {
                let heartbeat = poller.heartbeat.shared();
                loop {
                    futures::select! {

                        item = poller.stream.next().fuse() => match item {
                            Some(Ok(Some(req))) => {
                                let task_id = req.parts.task_id.clone();
                                match service.call(req).await {
                                    Ok(res) => {
                                        res_tx.send((task_id, Ok(format!("{res:?}")))).await.unwrap();
                                    },
                                    Err(err) => {
                                        res_tx.send((task_id, Err(err.into().to_string()))).await.unwrap();
                                    }
                                }
                            }
                            Some(Ok(None)) | None => break,
                            Some(Err(_e)) => {
                                // handle error
                                break;
                            }
                        },
                        _ = stop_rx.next().fuse() => break,
                        _ = heartbeat.clone().fuse() => {

                        },
                    }
                }
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
        };
    }
}
