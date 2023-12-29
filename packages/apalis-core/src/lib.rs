#![crate_name = "apalis_core"]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! # apalis-core
//! Utilities for building job and message processing tools.
//! This crate contains traits for working with workers.
//! ````rust
//! # use std::time::Duration;
//! # use apalis_core::builder::WorkerBuilder;
//! # use apalis_core::monitor::Monitor;
//! # use apalis_core::layers::tracing::TraceLayer;
//! # use apalis_core::context::JobContext;
//! # use apalis_core::storage::builder::WithStorage;
//! # use apalis_core::builder::WorkerFactoryFn;
//! # use apalis_core::job::Job;
//! # use apalis_sql::sqlite::SqliteStorage;
//! # use serde::{Deserialize, Serialize};
//! # #[derive(Debug, Deserialize, Serialize)]
//! # pub struct Email {
//! #     pub to: String,
//! #     pub subject: String,
//! #     pub text: String,
//! # }
//! # impl Job for Email {
//! #     const NAME: &'static str = "apalis::Email";
//! # }
//! pub async fn send_email(job: Email, _ctx: JobContext) {
//!    log::info!("Attempting to send email to {}", job.to);
//! }
//! async fn run() {
//!     let sqlite: SqliteStorage<Email> = SqliteStorage::connect("sqlite::memory:")
//!         .await
//!         .expect("unable to connect to sqlite");
//!     sqlite
//!         .setup()
//!         .await
//!         .expect("unable to run migrations for sqlite");
//!
//!     Monitor::new()
//!         .register_with_count(2, move |c| {
//!             WorkerBuilder::new(format!("tasty-banana-{c}"))
//!                 .layer(TraceLayer::new())
//!                 .with_storage(sqlite.clone())
//!                 .build_fn(send_email)
//!         })
//!         .shutdown_timeout(Duration::from_secs(1))
//!         // Here you could use tokio::ctrl_c etc
//!         .run_with_signal(async { Ok(()) }).await;
//! }
//! ````
//! ## How Workers Run and Monitored
//! `apalis` employs a robust system for running and monitoring workers, ensuring efficient and reliable execution of tasks. This section provides an overview of the underlying mechanism and how it can be utilized effectively.
//! 1. Worker Initialization.
//! To begin, the `Monitor::new()` function is called to create a new instance of the worker monitor. The monitor acts as a central control unit for managing and supervising the worker threads.
//! 2. Worker Registration.
//! Once the monitor is instantiated, workers can be registered using `register()` and `.register_with_count()` methods. The former takes in a single worker while the former method takes two parameters: the desired number of workers (count) and a closure `(move |_| { ... })` that specifies the worker logic.
//! Within the closure, a WorkerBuilder is utilized to construct individual worker instances. The WorkerBuilder provides a flexible and configurable way to set up worker-specific configurations, such as providing dependencies or applying additional layers to the worker.
//! 3. Worker Configuration
//! In the example code snippet, the WorkerBuilder is configured with a job source (like storage, message queue or stream) eg `SqliteStorage` and a TraceLayer to enable tracing capabilities. These configurations are specific to the needs of the workers being created.
//! 4. Worker Construction.
//! The `.build_fn(fn)` and `.build(service)` methods of the WorkerBuilder can then be invoked, eg specifying the function (send_email) that the worker will execute. This function represents the actual work to be performed by each worker. It can be a custom-defined function or a predefined function provided by the library.
//! 5. Worker Execution.
//! Upon completing the worker configuration, the worker is ready to be executed. The worker instance is added to the internal thread pool managed by the monitor. The monitor will ensure that the specified number of worker threads (count) are created and available for processing tasks.
//! 6. Worker Monitoring.
//! The monitor continuously monitors the worker threads to ensure their smooth operation. It keeps track of the workers' status, manages their lifecycle, and restarts any workers that may have encountered errors or terminated unexpectedly.
//! 7. Asynchronous Execution.
//! To facilitate asynchronous execution, the `.run().await` method is invoked on the monitor. This call suspends the current task until all workers have completed their execution. This is particularly useful when integrating the library into asynchronous Rust applications or frameworks.
//!
//! ## Middleware aka Layering
//! `apalis` prefers a functional approach to job handling and uses `tower::Layer` to model services as jobs.
//!
//! First, we need to define a tower service.
//! ```rust
//! # use std::task::{Context, Poll};
//! # use log::info;
//! # use tower::Service;
//! // This service implements the Log behavior
//! pub struct LogService<S> {
//!    target: &'static str,
//!    service: S,
//! }
//!
//! impl<S, Request> Service<Request> for LogService<S>
//! where
//!     S: Service<Request>,
//!     Request: std::fmt::Debug,
//! {
//!    type Response = S::Response;
//!    type Error = S::Error;
//!    type Future = S::Future;
//!
//!    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//!        self.service.poll_ready(cx)
//!    }
//!
//!    fn call(&mut self, request: Request) -> Self::Future {
//!        // Use service to apply middleware before or(and) after a request
//!        info!("request = {:?}, target = {:?}", request, self.target);
//!        self.service.call(request)
//!        // Also possible to do something after
//!    }
//!}
//! ```
//!
//! Then we define a layer.
//!
//! ```rust
//! # use std::task::{Context, Poll};
//! # use log::info;
//! # use tower::{Layer, Service};
//! # // This service implements the Log behavior
//! # pub struct LogService<S> {
//! #    target: &'static str,
//! #    service: S,
//! # }
//! # impl<S, Request> Service<Request> for LogService<S>
//! # where
//! #     S: Service<Request>,
//! #     Request: std::fmt::Debug,
//! # {
//! #    type Response = S::Response;
//! #    type Error = S::Error;
//! #    type Future = S::Future;
//! #    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//! #        self.service.poll_ready(cx)
//! #    }
//! #    fn call(&mut self, request: Request) -> Self::Future {
//! #        // Use service to apply middleware before or(and) after a request
//! #        info!("request = {:?}, target = {:?}", request, self.target);
//! #        self.service.call(request)
//! #        // Also possible to do something after
//! #    }
//! # }
//! pub struct LogLayer {
//!     target: &'static str,
//! }
//!
//! impl LogLayer {
//!    pub fn new(target: &'static str) -> Self {
//!        Self { target }
//!    }
//! }
//!
//! impl<S> Layer<S> for LogLayer {
//!     type Service = LogService<S>;
//!
//!     fn layer(&self, service: S) -> Self::Service {
//!         LogService {
//!             target: self.target,
//!             service,
//!         }
//!     }
//! }
//! ```
//! Layers are executed sequentially.
//!
//! ```ignore
//! .layer(LogLayer::new("log-layer-1"))
//! .layer(LogLayer::new("log-layer-2"))
//! ```
//! `log-layer-1` would be logged before `log-layer-2`.
//! This the means you should put your general layers first eg, `TraceLayer` and `CatchPanicLayer` should be before something like `AckLayer`
//! This also can affect how other layers behave. Eg Any layer before `TraceLayer` may do some tracing, but those traces would not appear in that job's tracing span.
//!
//! ## Graceful Shutdown
//! `apalis` allows optional opt-in to graceful shutdown. This can be added to `Monitor::run_with_signal` and this can be any future. We highly recommend using `tokio::signal::ctrl_c` or something similar.  

/// Represent utilities for creating worker instances.
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

/// Represents an executor. Currently tokio is implemented as default
pub mod executor;
/// Represents monitoring of running workers
pub mod monitor;
/// Represents extra utils needed for runtime agnostic approach
pub mod utils;
/// Represents the utils for building workers.
pub mod worker;

#[cfg(feature = "expose")]
#[cfg_attr(docsrs, doc(cfg(feature = "expose")))]
/// Utilities to expose workers and jobs to external tools eg web frameworks and cli tools
pub mod expose;

#[cfg(feature = "mq")]
#[cfg_attr(docsrs, doc(cfg(feature = "mq")))]
/// Message queuing utilities
pub mod mq;

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
                max_concurrent_jobs: 1000,
            },
        )
    }
}

#[cfg(feature = "chrono")]
use chrono::{DateTime, Utc};
#[cfg(feature = "time")]
use time::OffsetDateTime;

#[cfg(feature = "chrono")]
type Timestamp = DateTime<Utc>;
#[cfg(all(not(feature = "chrono"), feature = "time"))]
type Timestamp = OffsetDateTime;
