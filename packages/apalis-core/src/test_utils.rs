use crate::backend::{Backend, Reschedule};
use crate::builder::{WorkerBuilder, WorkerFactory};
use crate::error::BoxDynError;
use crate::request::Request;
use crate::task::task_id::TaskId;
use crate::worker::{Event, ReadinessService, TrackerService, Worker, WorkerError};
use futures::channel::mpsc::{self, channel, Receiver, Sender, TryRecvError};
use futures::future::BoxFuture;
use futures::stream::{BoxStream, Stream, StreamExt};
use futures::{FutureExt, SinkExt};
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

pub struct TestWorker<B, S, Res> {
    stream: BoxStream<'static, (TaskId, Result<Res, BoxDynError>)>,
    backend: PhantomData<B>,
    service: PhantomData<(S, Res)>,
}

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
    type Result = Option<(TaskId, Result<Res, BoxDynError>)>;
    async fn execute_next(&mut self) -> Self::Result {
        self.stream.next().await
    }
}

impl<B, S, Res> TestWorker<B, S, Res> {
    pub fn new<Args, Ctx>(backend: B, service: S) -> Self
    where
        B: Backend<Request<Args, Ctx>> + 'static,
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
        <<B as Backend<Request<Args, Ctx>>>::Layer as Layer<
            ReadinessService<TrackerService<TestEmitService<S, Res>>>,
        >>::Service: Service<Request<Args, Ctx>>,
        <<<B as Backend<Request<Args, Ctx>>>::Layer as Layer<
            ReadinessService<TrackerService<TestEmitService<S, Res>>>,
        >>::Service as Service<Request<Args, Ctx>>>::Error: Into<BoxDynError> + Sync + Send,
        <<<B as Backend<Request<Args, Ctx>>>::Layer as Layer<
            ReadinessService<TrackerService<TestEmitService<S, Res>>>,
        >>::Service as Service<Request<Args, Ctx>>>::Future: Send,
        <<B as Backend<Request<Args, Ctx>>>::Layer as Layer<
            ReadinessService<TrackerService<TestEmitService<S, Res>>>,
        >>::Service: std::marker::Send + 'static,
    {
        enum Item<R> {
            Ev(Result<Event, WorkerError>),
            Res((TaskId, Result<R, BoxDynError>)),
        }
        let (mut tx, rx) = channel(1);
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
            .map(|r| Item::Ev(r))
            .boxed();
        let task_stream = rx.map(|s| Item::Res(s));
        let stream = futures::stream::select(task_stream, stream)
            .filter_map(|s| async move {
                match s {
                    Item::Ev(_) => None,
                    Item::Res(r) => Some(r),
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

/// A generic backend wrapper that polls and executes jobs
#[derive(Debug)]
pub struct TestWrapper<B, Req, Res, Worker> {
    stop_tx: Sender<()>,
    res_rx: Receiver<(TaskId, Result<String, String>)>,
    _p: PhantomData<Req>,
    _r: PhantomData<Res>,
    should_next: Arc<AtomicBool>,
    /// The inner backend
    pub backend: B,
    /// The inner worker
    pub worker: Worker,
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

// impl<B, Req, Res, Ctx, Svc, Mw> TestWrapper<B, Request<Req, Ctx>, Res, Worker<Req, Ctx, B, Svc, Mw>>
// where
//     B: Backend<Request<Req, Ctx>> + Send + Sync + 'static + Clone,
//     Req: Send + 'static,
//     Ctx: Send,
//     B::Stream: Send + 'static,
//     B::Stream: Stream<Item = Result<Option<Request<Req, Ctx>>, BoxDynError>> + Unpin,
//     Res: Debug,
// {
//     /// Build a new instance provided a custom service
//     pub fn new_with_service(backend: B, service: Svc) -> (Self, BoxFuture<'static, ()>)
//     where
//         Svc::Future: Send,
//         Svc: Send + Sync + Service<Request<Req, Ctx>, Response = Res> + 'static,
//         Req: Send + Sync + 'static,
//         Svc::Response: Send + Sync + 'static,
//         Svc::Error: Send + Sync + std::error::Error,
//         Ctx: Send + Sync + 'static,
//         B: Backend<Request<Req, Ctx>> + 'static,
//         B::Layer: Layer<Svc>,
//         <B::Layer as Layer<Svc>>::Service: Service<Request<Req, Ctx>, Response = Res> + Send + Sync,
//         B::Stream: Unpin + Send,
//         Res: Send,
//         <<B::Layer as Layer<Svc>>::Service as Service<Request<Req, Ctx>>>::Future: Send,
//         <<B::Layer as Layer<Svc>>::Service as Service<Request<Req, Ctx>>>::Error:
//             Send + Sync + std::error::Error,
//         B::Layer: Layer<TestEmitService<Svc>>,
//         <B::Layer as Layer<TestEmitService<Svc>>>::Service:
//             Service<Request<Req, Ctx>, Response = Res> + Send + Sync,
//         <<B::Layer as Layer<TestEmitService<Svc>>>::Service as Service<Request<Req, Ctx>>>::Future:
//             Send,
//         <<B::Layer as Layer<TestEmitService<Svc>>>::Service as Service<Request<Req, Ctx>>>::Error:
//             Send + Sync + Sync + std::error::Error,
//     {
//         let (mut res_tx, res_rx) = channel(10);
//         let should_next = Arc::new(AtomicBool::new(false));

//         // let worker = WorkerBuilder::new("test-worker")
//         //     .backend(backend.clone())
//         //     .build(service);
//         let handle = todo!();
//         let (stop_tx, mut stop_rx) = channel::<()>(1);

//         let poller = async move {
//             todo!();
//             // let worker = worker.run().shared();
//             // loop {
//             //     futures::select! {
//             //         _ = stop_rx.next().fuse() => break,
//             //         _ = worker.clone().fuse() => {

//             //         },
//             //     }
//             // }
//             // res_tx.close_channel();
//         };
//         (
//             TestWrapper {
//                 stop_tx,
//                 res_rx,
//                 _p: PhantomData,
//                 backend,
//                 _r: PhantomData,
//                 should_next,
//                 worker: handle,
//             },
//             poller.boxed(),
//         )
//     }

//     /// Stop polling
//     pub fn stop(mut self) {
//         self.stop_tx.try_send(()).unwrap();
//     }

//     /// Gets the current state of results
//     pub async fn execute_next(&mut self) -> Option<(TaskId, Result<String, String>)> {
//         self.should_next.store(true, Ordering::Release);
//         self.res_rx.next().await
//     }

//     /// Gets the current state of results
//     pub fn try_execute_next(
//         &mut self,
//     ) -> Result<Option<(TaskId, Result<String, String>)>, TryRecvError> {
//         self.should_next.store(true, Ordering::Release);
//         self.res_rx.try_next().map(|res| {
//             self.should_next.store(false, Ordering::Release);
//             res
//         })
//     }
// }

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
    use std::time::Duration;

    use futures::io;

    use crate::{
        backend::Push,
        builder::WorkerBuilder,
        error::BoxDynError,
        memory::MemoryStorage,
        request::Request,
        service_fn::service_fn,
        test_utils::{ExecuteNext, TestWorker},
        worker::{self, WorkerContext},
    };

    #[tokio::test]
    async fn it_works() {
        let mut backend = MemoryStorage::new();

        for i in 0..10 {
            backend.push(i).await.unwrap();
        }

        let service = service_fn(|req: u32, w: WorkerContext| async move {
            if (req == 9) {
                w.stop()?;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok::<_, BoxDynError>(req)
        });
        let mut worker = TestWorker::new(backend, service);

        let (_, res) = worker.execute_next().await.unwrap();
        let req = res.unwrap_err();
        // assert_eq!(req, 0);
    }
}

pub use tower::service_fn as apalis_test_service_fn;

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
