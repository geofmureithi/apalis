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
use std::sync::Arc;

use futures::Stream;
use poller::Poller;
use worker::WorkerId;

/// Represent utilities for creating worker instances.
pub mod builder;
/// Includes all possible error types.
pub mod error;
/// Represents an executor.
pub mod executor;
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
pub trait Backend<Req> {
    /// The stream to be produced by the backend
    type Stream: Stream<Item = Result<Option<Req>, crate::error::Error>>;

    /// Returns the final decoration of layers
    type Layer;

    /// Returns a poller that is ready for streaming
    fn poll(self, worker: WorkerId) -> Poller<Self::Stream, Self::Layer>;
}

/// This allows encoding and decoding of requests in different backends
pub trait Codec<T, Compact> {
    /// Error encountered by the codec
    type Error;

    /// Convert to the compact version
    fn encode(&self, input: &T) -> Result<Compact, Self::Error>;

    /// Decode back to our request type
    fn decode(&self, compact: &Compact) -> Result<T, Self::Error>;
}

/// A boxed codec
pub type BoxCodec<T, Compact, Error = error::Error> =
    Arc<Box<dyn Codec<T, Compact, Error = Error> + Sync + Send + 'static>>;

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

/// Test utilities that allows you to test backends
pub mod test_utils {
    use crate::error::{BoxDynError};

    use crate::request::Request;

    use crate::task::task_id::TaskId;
    use crate::worker::WorkerId;
    use crate::Backend;
    use futures::channel::mpsc::{channel, Sender};
    use futures::stream::{Stream, StreamExt};
    use futures::{Future, FutureExt, SinkExt};
    use std::collections::HashMap;
    use std::fmt::Debug;
    use std::marker::PhantomData;
    use std::ops::{Deref, DerefMut};
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::thread;
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
    pub struct TestWrapper<B, Req> {
        stop_tx: Sender<()>,
        executions: Arc<Mutex<HashMap<TaskId, Result<String, String>>>>,
        _p: PhantomData<Req>,
        backend: B,
    }

    impl<B: Clone, Req> Clone for TestWrapper<B, Req> {
        fn clone(&self) -> Self {
            TestWrapper {
                stop_tx: self.stop_tx.clone(),
                executions: Arc::clone(&self.executions),
                _p: PhantomData,
                backend: self.backend.clone(),
            }
        }
    }

    impl<B, Req> TestWrapper<B, Req>
    where
        B: Backend<Request<Req>> + Send + Sync + 'static + Clone,
        Req: Send + 'static,
        B::Stream: Send + 'static,
        B::Stream: Stream<Item = Result<Option<Request<Req>>, crate::error::Error>> + Unpin,
    {
        /// Build a new instance provided a custom service
        pub fn new_with_service<S>(backend: B, service: S) -> Self
        where
            S: Service<Request<Req>> + Send + 'static,
            B::Layer: Layer<S>,
            <<B as Backend<Request<Req>>>::Layer as Layer<S>>::Service: Service<Request<Req>> + Send + 'static,
            <<<B as Backend<Request<Req>>>::Layer as Layer<S>>::Service as Service<Request<Req>>>::Response: Debug,
            <<<B as Backend<Request<Req>>>::Layer as Layer<S>>::Service as Service<Request<Req>>>::Error: Send + Into<BoxDynError> + Sync
        {
            let worker_id = WorkerId::new("test-worker");
            let b = backend.clone();
            let mut poller = b.poll(worker_id);
            let (stop_tx, mut stop_rx) = channel::<()>(1);

            let mut service = poller.layer.layer(service);

            let executions: Arc<Mutex<HashMap<TaskId, Result<String, String>>>> =
                Default::default();
            let executions_clone = executions.clone();
            thread::spawn(move || {
                futures::executor::block_on(async move {
                    let heartbeat = poller.heartbeat.shared();
                    loop {
                        futures::select! {

                            item = poller.stream.next().fuse() => match item {
                                Some(Ok(Some(req))) => {

                                    let task_id = req.get::<TaskId>().cloned().expect("Request does not contain Task_ID");
                                    // handle request
                                    match service.call(req).await {
                                        Ok(res) => {
                                            executions_clone.lock().unwrap().insert(task_id, Ok(format!("{res:?}")));
                                        },
                                        Err(err) => {
                                            executions_clone.lock().unwrap().insert(task_id, Err(err.into().to_string()));
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
                });
            });

            Self {
                stop_tx,
                executions,
                _p: PhantomData,
                backend,
            }
        }

        /// Stop polling
        pub fn stop(mut self) {
            let _ = self.stop_tx.send(());
        }

        /// Gets the current state of results
        pub fn get_results(&self) -> HashMap<TaskId, Result<String, String>> {
            self.executions.lock().unwrap().clone()
        }
    }

    impl<B, Req> Deref for TestWrapper<B, Req>
    where
        B: Backend<Request<Req>>,
    {
        type Target = B;

        fn deref(&self) -> &Self::Target {
            &self.backend
        }
    }

    impl<B, Req> DerefMut for TestWrapper<B, Req>
    where
        B: Backend<Request<Req>>,
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
                let service = apalis_test_service_fn(|request: Request<u32>| async {
                    Ok::<_, io::Error>(request)
                });
                let mut t = TestWrapper::new_with_service(backend, service);
                let res = t.get_results();
                assert_eq!(res.len(), 0); // No job is done
                t.enqueue(1).await.unwrap();
                tokio::time::sleep(Duration::from_secs(1)).await;
                let res = t.get_results();
                assert_eq!(res.len(), 1); // One job is done
            }
        };
    }
    #[macro_export]
    /// Tests a generic storage
    macro_rules! test_storage {
        ($backend_instance:expr) => {
            #[tokio::test]
            async fn it_works_as_a_storage_backend() {
                let backend = $backend_instance;
                let service = apalis_test_service_fn(|request: Request<u32>| async {
                    Ok::<_, io::Error>(request)
                });
                let mut t = TestWrapper::new_with_service(backend, service);
                let res = t.get_results();
                assert_eq!(res.len(), 0); // No job is done
                t.push(1).await.unwrap();
                ::apalis_core::sleep(Duration::from_secs(1)).await;
                let res = t.get_results();
                assert_eq!(res.len(), 1); // One job is done
            }
        };
    }
}
