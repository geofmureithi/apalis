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
//! A high-performance, type-safe task processing framework for rust.
//!
//! `apalis-core` provides the fundamental abstractions and runtime components for building
//! scalable background task systems with middleware support, graceful shutdown, and monitoring capabilities.
//!
//! This is advanced documentation, for guide level documentation is found on the [website](https://apalis.dev).
//!
//! # Core Concepts
//!
//! `apalis-core` is built around four primary abstractions that provide a flexible and extensible task processing system:
//!
//! - **[`Tasks`](#tasks)**: Type-safe task data structures with processing metadata
//! - **[`Backends`](#backends)**: Pluggable task storage and streaming implementations  
//! - **[`Workers`](#workers)**: Task processing engines with lifecycle management
//! - **[`Monitor`](#monitor)**: Multi-worker coordination and observability
//!
//! The framework leverages the `tower` service abstraction to provide a rich middleware
//! ecosystem like error handling, timeouts, rate limiting,
//! and observability.
//!
//!
//! ## Tasks
//!
//! The task struct provides type-safe components for task data and metadata:
//! - [`Args`](crate::task_fn::guide) - The primary structure for the task
//! - [`Parts`](crate::task::Parts) - Wrapper type for information for task execution includes context, status, attempts, task_id and metadata
//! - [`Context`](crate::backend::Backend#required-associated-types) - contextual information with the task provided by the backend
//! - [`Status`](crate::task::status::Status) - Represents the current state of a task
//! - [`TaskId`](crate::task::task_id::TaskId) - Unique identifier for task tracking
//! - [`Attempt`](crate::task::attempt::Attempt) - Retry tracking and attempt information
//! - [`Extensions`](crate::task::data) - Type-safe storage for additional task data
//! - [`Metadata`](crate::task::metadata) - metadata associated with the task
//!
//! ### Example: Using `TaskBuilder`
//!
//! ```ignore
//! let task: Task<String, ()> = TaskBuilder::new("my-task".to_string())
//!     .id("task-123".into())
//!     .attempts(3)
//!     .timeout(Duration::from_secs(30))
//!     .run_in_minutes(10)
//!     .build();
//! ```
//! Specific documentation for tasks can be found in the [`task`] and [`task::builder`] modules.
//!
//! #### Relevant Guides:
//! - [**Defining Task arguments**](crate::task_fn::guide) - Creating effective task arguments that are scalable and type-safe
//!
//! ## Backends
//!
//! The [`Backend`](crate::backend::Backend) trait serves as the core abstraction for all task sources.
//! It defines task polling mechanisms, streaming interfaces, and middleware integration points.
//!
//! <details>
//! <summary>Associated Types:</summary>
//!
//! - `Stream` - Defines the task stream type for polling operations
//! - `Layer` - Specifies the middleware layer stack for the backend
//! - `Codec` - Determines serialization format for task data persistence
//! - `Beat` - Heartbeat stream for worker liveness checks
//! - `IdType` - Type used for unique task identifiers
//! - `Ctx` -   Context associated with tasks
//! - `Error` - Error type for backend operations
//!
//! </details>
//!
//! ### Inbuilt Implementations
//! - [`MemoryStorage`](crate::backend::memory::MemoryStorage) : In-memory storage based on channels
//! - [`Pipe`](crate::backend::pipe) : Pipe-based backend for a stream-to-backend pipeline
//! - [`CustomBackend`](crate::backend::custom) : Flexible backend composition allowing custom functions for task management
//!
//! Backends handle task persistence, distribution, and reliability concerns while providing
//! a uniform interface for worker consumption.
//!
//! ## Workers
//!
//! The [`Worker`](crate::worker) is the core runtime component responsible for task polling, execution, and lifecycle management:
//!
//! ### Worker Lifecycle
//!
//! - Workers are responsible for task polling, processing, and lifecycle management.
//! - Workers can be run as a future or as a stream of events.
//! - Workers readiness is conditioned on the backend and service (and middleware) being ready.
//! - This means any blocking middleware eg (concurrency) will block the worker from polling tasks.
//!
//! ### Worker Components
//!
//! The following are the main components the worker module:
//!
//! - [`WorkerBuilder`] - Fluent builder for configuring and constructing workers
//! - [`Worker`] - Actual worker implementation that processes tasks
//! - [`WorkerContext`] - Runtime state including task counts and execution status
//! - [`Event`] - Worker event enumeration (`Start`, `Engage`, `Idle`, `Error`, `Stop`)
//! - [`Ext`](crate::worker::ext) - Extension traits and middleware for adding functionality to workers
//!
//! ### Example: Building and Running a Worker
//! ```rust
//! # use apalis_core::worker::{builder::WorkerBuilder, context::WorkerContext};
//! # use apalis_core::backend::memory::MemoryStorage;
//! # use apalis_core::error::BoxDynError;
//! # use std::time::Duration;
//! # use crate::apalis_core::worker::ext::event_listener::EventListenerExt;
//! # use crate::apalis_core::backend::TaskSink;
//! #[tokio::main]
//! async fn main() {
//!     let mut in_memory = MemoryStorage::new();
//!     in_memory.push(1u32).await.unwrap();
//!
//!     async fn task(
//!         task: u32,
//!         worker: WorkerContext,
//!     ) -> Result<(), BoxDynError> {
//!          /// Do some work
//!         tokio::time::sleep(Duration::from_secs(1)).await;
//!         worker.stop().unwrap();
//!         Ok(())
//!     }
//!
//!     let worker = WorkerBuilder::new("rango-tango")
//!         .backend(in_memory)
//!         .on_event(|ctx, ev| {
//!             println!("On Event = {:?}, {:?}", ev, ctx.name());
//!         })
//!         .build(task);
//!     worker.run().await.unwrap();
//! }
//! ```
//!
//! Learn more about workers in the [`worker`](crate::worker) and [`worker::builder`](crate::worker::builder) modules.
//!
//! #### Relevant Tutorials:
//! - [**Creating task handlers**](crate::task_fn::guide) - Defining task processing functions using the [`TaskFn`] trait
//! - [**Testing task handlers with `TestWorker`**](crate::worker::test_worker) - Specialized worker implementation for unit and integration testing
//!
//! ## Monitor
//!
//! The [`Monitor`](crate::monitor::Monitor) helps manage and coordinate multiple workers:
//!
//! **Main Features:**
//! - **Worker Registry** - Keeps track of active workers
//! - **Event Handling** - Handles and processes worker events
//! - **Graceful Shutdown** - Stops all workers together safely
//! - **Health Monitoring** - Restarts and manages worker health
//! ### Example: Using `Monitor` with a Worker
//!
//! ```rust
//! # use apalis_core::monitor::Monitor;
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_core::backend::json::JsonStorage;
//! # use apalis_core::task::Task;
//! # use apalis_core::backend::TaskSink;
//! # use tower::service_fn;
//! # use std::time::Duration;
//! # use apalis_core::worker::context::WorkerContext;
//! #[tokio::main]
//! async fn main() {
//!     let mut storage = JsonStorage::new_temp().unwrap();
//!     storage.push(1u32).await.unwrap();
//!
//!     let monitor = Monitor::new()
//!         .on_event(|ctx, event| println!("{}: {:?}", ctx.name(), event))
//!         .register(move |_| {
//!             WorkerBuilder::new("demo-worker")
//!                 .backend(storage.clone())
//!                 .build(|req: u32, ctx: WorkerContext| async move {
//!                     println!("Processing task: {:?}", req);
//! #                    ctx.stop().unwrap();
//!                     Ok::<_, std::io::Error>(req)
//!                 })
//!         });
//!
//!     // Start monitor and run all registered workers
//!     monitor.run().await.unwrap();
//! }
//! ```
//!
//! Learn more about the monitor in the [`monitor` module](crate::monitor).
//!
//! ## Middleware
//!
//! Built on the `tower` ecosystem, `apalis-core` provides extensive middleware support like error handling, timeouts, rate limiting, and observability.
//!
//! ### Core Middleware
//!
//! The following middleware layers are included with their worker extensions:
//! - [`AcknowledgmentLayer`] - Task acknowledgment after processing
//! - [`EventListenerLayer`] - Worker event emission and handling
//! - [`CircuitBreakerLayer`] - Circuit breaker pattern for failure handling
//! - [`LongRunningLayer`] - Support for tracking long-running tasks
//!
//! ### Extending with middleware
//!
//! You can write your own middleware to run code before or after a task is processed.
//!
//! <details>
//! <summary>Creating Custom Middleware</summary>
//!
//! Here's a simple example of a logging middleware layer:
//!
//! ```rust
//! use apalis_core::task::Task;
//! use tower::{Layer, Service};
//! use std::task::{Context, Poll};
//!
//! // Define a logging service that wraps another service
//! pub struct LoggingService<S> {
//!     inner: S,
//! }
//!
//! impl<S, Req, Res, Err> Service<Task<Req, ()>> for LoggingService<S>
//! where
//!     S: Service<Task<Req, ()>, Response = Res, Error = Err>,
//!     Req: std::fmt::Debug,
//! {
//!     type Response = Res;
//!     type Error = Err;
//!     type Future = S::Future;
//!
//!     fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//!         self.inner.poll_ready(cx)
//!     }
//!
//!     fn call(&mut self, req: Task<Req, ()>) -> Self::Future {
//!         println!("Processing task: {:?}", req.args);
//!         self.inner.call(req)
//!     }
//! }
//!
//! // Define a layer that wraps services with LoggingService
//! pub struct LoggingLayer;
//!
//! impl<S> Layer<S> for LoggingLayer {
//!     type Service = LoggingService<S>;
//!
//!     fn layer(&self, service: S) -> Self::Service {
//!         LoggingService { inner: service }
//!     }
//! }
//! ```
//! </details>
//!
//! If you want your middleware to do more than just intercept requests and responses, you can use extension traits. See the [`worker::ext`](crate::worker::ext) module for examples.
//!
//! ## Error Handling
//!
//! `apalis-core` defines a comprehensive error taxonomy for robust error handling:
//!
//! - [`AbortError`] - Non-retryable fatal errors requiring immediate termination
//! - [`RetryAfterError`] - Retryable execution errors triggering retry mechanisms after a delay
//! - [`DeferredError`] - Retryable execution errors triggering immediate retry
//!
//! This error classification enables precise error handling strategies and
//! appropriate retry behavior for different failure scenarios.
//!
//! ## Graceful Shutdown
//!
//! `apalis-core` has a reliable graceful shutdown system that makes sure
//! workers stop safely and all tasks finish before shutting down:
//!
//! **Key Features:**
//! - Task tracking: Workers keep track of how many tasks are running.
//! - Shutdown control: The system waits until all tasks are finished before shutting down.
//! - Monitor coordination: A shared [`Shutdown`] token helps all workers stop together.
//! - Timeout: You can set a time limit for shutdown using [`with_terminator`](crate::monitor::Monitor::with_terminator).
//!
//! Learn more about the graceful shutdown process in the [`monitor`](crate::monitor#graceful-shutdown-with-timeout) module.
//!
//! # Feature flags
#![cfg_attr(
    feature = "docsrs",
    cfg_attr(doc, doc = ::document_features::document_features!())
)]
//!
//! # Development
//! `apalis-core` provides comprehensive extensibility mechanisms such as middleware and ext traits.
//!  Beyond there one may want to dive deeper into the following topics:
//!
//! - [**Using CustomBackend**](crate::backend::custom) - using custom backend to integrate with already existing systems
//! - [**Implementing Backends**](crate::backend::guide) - implementing the [`Backend`] trait from scratch
//! - [**Extending Workers using extension traits**](crate::worker::ext#creating-a-custom-worker-extension-trait) - implementing custom worker functionality via extension traits
//!
//! # Observability
//! You can track tasks using [apalis-board](https://github.com/apalis-dev/apalis-board).
//! ![Task](https://github.com/apalis-dev/apalis-board/raw/master/screenshots/task.png)
//! 
//! [`Backend`]: crate::backend::Backend
//! [`TaskFn`]: crate::task_fn::TaskFn
//! [`Service`]: tower_service::Service
//! [`Task`]: crate::task
//! [`WorkerBuilder`]: crate::worker::builder
//! [`Worker`]: crate::worker
//! [`Monitor`]: crate::monitor
//! [`AcknowledgmentLayer`]: crate::worker::ext::ack
//! [`EventListenerLayer`]: crate::worker::ext::event_listener
//! [`CircuitBreakerLayer`]: crate::worker::ext::circuit_breaker
//! [`LongRunningLayer`]: crate::worker::ext::long_running
//! [`AbortError`]: crate::error::AbortError
//! [`RetryAfterError`]: crate::error::RetryAfterError
//! [`DeferredError`]: crate::error::DeferredError
//! [`WorkerContext`]: crate::worker::context::WorkerContext
//! [`Event`]: crate::worker::event::Event
//! [`Parts`]: crate::task::Parts
//! [`Status`]: crate::task::status::Status
//! [`TaskId`]: crate::task::task_id::TaskId
//! [`Attempt`]: crate::task::attempt::Attempt
//! [`FromRequest`]: crate::task_fn::FromRequest
//! [`TestWorker`]: crate::worker::test_worker::TestWorker
//! [`Shutdown`]: crate::monitor::shutdown::Shutdown

pub mod backend;
/// Includes internal error types.
pub mod error;
#[macro_use]
pub(crate) mod macros;
/// Actively manage and observe workers
pub mod monitor;
pub mod task;
pub mod task_fn;
pub mod worker;

/// Layers for building middleware stacks
pub mod layers {
    pub use tower_layer::*;
    pub use tower_service::Service;
}
/// Timing and delaying utilities
#[cfg(feature = "sleep")]
pub mod timer {
    pub use futures_timer::Delay;
    /// Runtime agnostic sleep function based on [Delay]
    pub async fn sleep(duration: std::time::Duration) {
        futures_timer::Delay::new(duration).await;
    }
}
