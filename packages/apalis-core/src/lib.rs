#![crate_name = "apalis_core"]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! # apalis Core
//! Utilities for building job and message processing tools.

/// Represent utilities for creating [`Worker`] instances.
///
/// [`Worker`]: apalis_core::worker::Worker
pub mod builder;
/// Represents the [`JobContext`].
pub mod context;
/// Includes all possible error types.
pub mod error;
/// Includes the utilities for a job.
pub mod job;
/// Represents a service that is created from a function.
pub mod job_fn;
/// Represents middleware offered through [`tower::Layer`]
pub mod layers;
/// Represents the job bytes.
pub mod request;
/// Represents different possible responses.
pub mod response;

#[cfg(feature = "storage")]
#[cfg_attr(docsrs, doc(cfg(feature = "storage")))]
/// Represents ability to persist and consume jobs from storages.
pub mod storage;

/// Represents an async executor. Currently tokio is implemented as default
pub mod executor;
/// Represents monitoring of running workers
pub mod monitor;
/// Represents extra utils needed for runtime agnostic approach
pub mod utils;
/// Represents the actual executor of a [Job].
pub mod worker;

#[cfg(feature = "expose")]
#[cfg_attr(docsrs, doc(cfg(feature = "expose")))]
/// Utilities to expose workers and jobs to external tools eg web frameworks and cli tools
pub mod expose;

/// apalis mocking utilities
#[cfg(feature = "tokio-comp")]
pub mod mock {
    use futures::channel::mpsc::{Receiver, Sender};
    use futures::{Stream, StreamExt};
    use tower::Service;

    use crate::{
        job::Job,
        worker::{ready::ReadyWorker, WorkerId},
    };

    fn build_stream<Req: Send + 'static>(mut rx: Receiver<Req>) -> impl Stream<Item = Req> {
        let stream = async_stream::stream! {
            while let Some(item) = rx.next().await {
                yield item;
            }
        };
        stream.boxed()
    }

    /// Useful for mocking a worker usually for testing purposes
    ///
    /// # Example
    /// ```rust
    /// #[tokio::test(flavor = "current_thread")]
    /// async fn test_worker() {
    ///     let (handle, mut worker) = mock_worker(job_fn(job2));
    ///     handle.send(TestJob(Utc::now())).await.unwrap();
    ///     let res = worker.consume_next().await;
    /// }
    /// ```
    pub fn mock_worker<S, Req>(service: S) -> (Sender<Req>, ReadyWorker<impl Stream<Item = Req>, S>)
    where
        S: Service<Req>,
        Req: Job + Send + 'static,
    {
        let (tx, rx) = futures::channel::mpsc::channel(10);
        let stream = build_stream(rx);
        (
            tx,
            ReadyWorker {
                service,
                stream,
                id: WorkerId::new("mock-worker"),
                beats: Vec::new(),
            },
        )
    }
}
