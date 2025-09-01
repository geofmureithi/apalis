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

//! A high-performance, type-safe task processing framework built on top of the `tower` ecosystem.
//!
//! `apalis-core` provides the fundamental abstractions and runtime components for building
//! scalable background task systems with middleware support, graceful shutdown, and
//! comprehensive monitoring capabilities.
//!
//! This is technical documentation, for guide level documentation is found on the [website](https://apalis.dev).
//!
//! ## Features
//!
//! - **Type Safety** - Compile-time guarantees for task data and processing pipelines
//! - **Tower Integration** - Full middleware ecosystem compatibility
//! - **Async/Await** - Modern async runtime support with efficient resource utilization
//! - **Graceful Shutdown** - Clean termination with task completion guarantees
//! - **Extensibility** - Pluggable architecture for custom backends and middleware
//! - **Observability** - Comprehensive event system and tracing integration
//!
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
//! ecosystem for cross-cutting concerns like error handling, timeouts, rate limiting,
//! and observability.
//!
//!
//! ## Tasks
//!
//! The task struct provides type-safe components for task data and metadata:
//! - [`Args`] - The primary data payload for the task
//! - [`Meta`] - metadata associated with the task provided by the backend
//! - [`ExecutionContext`](crate::task::ExecutionContext) - Contextual information for task execution
//! - [`Status`](crate::task::Status) - Represents the current state of a task
//! - [`TaskId`](crate::task::task_id::TaskId) - Unique identifier for task tracking
//! - [`Attempt`](crate::task::attempt::Attempt) - Retry tracking and attempt information
//! - [`Extensions`](crate::task::data::Extensions) - Type-safe storage for additional task data
//!
//! ### Example: Using `TaskBuilder`
//!
//! ```rust
//! use apalis_core::task::{Task, TaskBuilder};
//!
//! let task: Task<String, ()> = TaskBuilder::new("my-task".to_string())
//!     .id("task-123".into())
//!     .with_status(Status::Pending)
//!     .run_in_minutes(10)
//!     .build();
//! ```
//! Specific documentation for tasks can be found in the [`task` module](crate::task) and [`task::builder`](crate::task::builder).
//! 
//! ### Tutorials
//! - [**Defining Task arguments**](crate::task::args::tutorial) - Creating effective task arguments that are scalable and type-safe

//!
//! ## Backends
//!
//! The [`Backend`](crate::backend::Backend) trait serves as the core abstraction for all task sources.
//! It defines task polling mechanisms, streaming interfaces, and middleware integration points. In other frameworks,
//! this concept is often referred to as a "queue" or "broker", but `apalis` uses the more general term "backend".
//!
//! **Associated Types:**
//! - `Stream` - Defines the task stream type for polling operations
//! - `Layer` - Specifies the middleware layer stack for the backend
//! - `Codec` - Determines serialization format for task data persistence
//! - `Beat` - Heartbeat stream for worker liveness checks
//! - `IdType` - Type used for unique task identifiers
//! - `Meta` - Metadata type associated with tasks
//! - `Error` - Error type for backend operations
//!
//! Backends handle task persistence, distribution, and reliability concerns while providing
//! a uniform interface for worker consumption.
//!
//! ## Workers
//!
//! Workers form the core execution engine, responsible for task polling, processing, and lifecycle management.
//! Workers can be run as a future or as a stream of events. Workers readiness is conditioned on the backend and service being ready.
//! This means any blocking middleware eg (concurrency) will block the worker from polling tasks.
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
//! # use apalis_core::worker::{WorkerBuilder, WorkerContext};
//! # use apalis_core::backend::memory::MemoryStorage;
//! # use apalis_core::error::BoxDynError;
//! # use std::time::Duration;
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
//! Learn more about workers in the [`worker` module](crate::worker) and [`worker::builder`](crate::worker::builder).
//! 
//! ### Tutorials
//! - [**Creating Task Functions**](crate::utils::task_fn::tutorial) - Defining task processing functions using the [`TaskFn`] trait
//! - [**Testing `task_fns` with `TestWorker`**](crate::worker::test_worker) - Specialized worker implementation for unit and integration testing
//!
//! ## Monitor
//!
//! The [`Monitor`](crate::monitor::Monitor) component provides centralized coordination
//! for multiple workers with comprehensive lifecycle management:
//!
//! **Core Responsibilities:**
//! - **Worker Registry** - Active worker tracking via `register
//! - **Event Handling** - Centralized event processing and subscription
//! - **Graceful Shutdown** - Coordinated worker termination with signal propagation
//! - **Health Monitoring** - Worker readiness and state tracking
//! ### Example: Using `Monitor` with a Worker
//!
//! ```rust
//! # use apalis_core::monitor::Monitor;
//! # use apalis_core::worker::WorkerBuilder;
//! # use apalis_core::memory::MemoryStorage;
//! # use apalis_core::task::Task;
//! # use tower::service_fn;
//! # use std::time::Duration;
//! #[tokio::main]
//! async fn main() {
//!     let mut storage = MemoryStorage::new();
//!     storage.push(1u32).await.unwrap();
//!
//!     let monitor = Monitor::new()
//!         .on_event(|ctx, event| println!("{}: {:?}", ctx.id(), event))
//!         .register(|_| {
//!             WorkerBuilder::new("demo-worker")
//!                 .backend(storage.clone())
//!                 .build(|req: u32| async move {
//!                     println!("Processing task: {:?}", req);
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
//! Built on the Tower ecosystem, `apalis-core` provides extensive middleware support
//! for cross-cutting concerns:
//!
//! ### Core Middleware
//! - [`AcknowledgmentLayer`] - Task acknowledgment after processing
//! - [`EventListenerLayer`] - Worker event emission and handling
//! - [`CircuitBreakerLayer`] - Circuit breaker pattern for failure handling
//! - [`LongRunningLayer`] - Support for tracking long-running tasks
//!
//! Each middleware layer integrates seamlessly with the Tower service stack,
//! providing composable and configurable task processing pipelines.
//!
//! #### Custom Middleware
//!
//! You can create your own Tower `Layer` and `Service` to process `Task` objects.
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
//!         println!("Processing task: {:?}", req.data());
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
//! 
//! Learn more about worker middleware in the [`worker::ext` module](crate::worker::ext).
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
//! The framework implements a sophisticated graceful shutdown system ensuring
//! clean worker termination and task completion:
//!
//! **Components:**
//! - **Task Tracking** - [`WorkerContext`] maintains accurate active task counts
//! - **Context Future** - Resolves when shutdown conditions are met and no tasks remain
//! - **Monitor Coordination** - Shared `Shutdown` tokens orchestrate multi-worker termination
//! - **Timeout Support** - Configurable termination timeouts via [`with_terminator()`]
//!
//! This system ensures data integrity and prevents task loss during application shutdown.
//!
//! # Feature flags
#![doc = document_features::document_features!()]
//!
//! # Development
//! 
//! `apalis` encourages contributions and custom extensions to fit diverse use cases.
//! `apalis-core` provides comprehensive testing utilities and extensibility mechanisms:
//!
//! - [**Implementing Backends**](crate::backend::tutorial) - Creating custom backends by implementing the [`Backend`] trait
//! - [**Extending Workers using extension traits**](crate::worker::ext#creating-a-custom-worker-extension-trait) - Implementing custom worker functionality via extension traits
//!
//! [`Backend`]: crate::backend::Backend
//! [`TaskFn`]: crate::task_fn::TaskFn
//! [`Service`]: tower_service::Service
//! [`Task`]: crate::task::Task
//! [`WorkerBuilder`]: crate::worker::builder::WorkerBuilder
//! [`Worker`]: crate::worker::Worker
//! [`Monitor`]: crate::monitor::Monitor
//! [`AcknowledgmentLayer`]: crate::worker::ext::ack::AcknowledgeLayer
//! [`TrackerLayer`]: crate::worker::ext::tracker::TrackerLayer
//! [`EventListenerLayer`]: crate::worker::ext::event_listener::EventListenerLayer
//! [`CircuitBreakerLayer`]: crate::worker::ext::circuit_breaker::CircuitBreakerLayer
//! [`LongRunningLayer`]: crate::worker::ext::long_running::LongRunningLayer
//! [`AbortError`]: crate::error::AbortError
//! [`RetryAfterError`]: crate::error::RetryAfterError
//! [`DeferredError`]: crate::error::DeferredError
//! [`WorkerContext`]: crate::worker::WorkerContext
//! [`Event`]: crate::worker::Event
//! [`ExecutionContext`]: crate::task::ExecutionContext
//! [`Status`]: crate::task::status::Status
//! [`TaskId`]: crate::task::task_id::TaskId
//! [`Attempt`]: crate::task::attempt::Attempt
//! [`Extensions`]: crate::task::data::Extensions
//! [`FromRequest`]: crate::util::FromRequest
//! [`TestWorker`]: crate::worker::test_worker::TestWorker
pub mod backend;
/// Includes internal error types.
pub mod error;
#[macro_use]
pub(crate) mod macros;
/// Utilities for managing and observing workers
pub mod monitor;
pub mod task;
pub mod util;
pub mod worker;
/// Represents timing and delaying utilities
#[cfg(feature = "sleep")]
pub mod timer {
    pub use futures_timer::Delay;
    /// shorthand future for sleeping
    pub async fn sleep(duration: std::time::Duration) {
        futures_timer::Delay::new(duration).await;
    }
}
