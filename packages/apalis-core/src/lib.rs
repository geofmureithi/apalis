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
//! Utilities for building background task processing tools.
//!
//! `apalis-core` provides foundational types and traits for building distributed, asynchronous background task processing systems.
//! It enables defining, scheduling, and monitoring tasks in a modular and extensible way.
//!
//! ## Concepts
//!
//! ### Request
//! A [`Request<Args, Context>`] represents a unit of work to be executed by a worker. It contains:
//! - `args`: the input to the task (e.g., an email to send).
//! - `parts`: metadata such as the task ID, execution attempts, state, backend context, and extensions.
//!
//! You can construct requests using:
//!
//! ```rust
//! use apalis_core::request::Request;
//! struct BackendContext;
//! let req = Request::new_with_ctx("send-email", BackendContext);
//! ```
//!
//! ### WorkerBuilder
//! [`WorkerBuilder`] provides a fluent API to configure workers with middlewares, filters, and error handling strategies.
//!
//! ```rust
//! use apalis_core::worker::WorkerBuilder;
//!
//! let worker = WorkerBuilder::new("my-worker")
//!     .layer(my_middleware)
//!     .build(handler);
//! ```
//! ### Worker
//! A [`Worker`] represents a task processor. It polls tasks from a [`Backend`], sends them to a [`Service`], and handles retry logic, timeouts, and state transitions.
//!
//! Workers can be created manually or using a [`WorkerBuilder`] for more fluent composition.
//!
//! ### ServiceFn
//! A [`ServiceFn`] is an adapter that turns an async function into a task [`Service`]. This is useful for injecting lightweight task handlers.
//!
//! Example:
//! ```rust
//! use apalis_core::service_fn;
//!
//! let handler = service_fn(|req: String, worker: WorkerContext | async move {
//!     println!("Processing task: {:?}", req);
//!     Ok(())
//! });
//! ```
//! ### Monitor
//! A [`Monitor`] supervises multiple [`Worker`]s, supports graceful shutdowns, and coordinates global signals like shutdown or restart
//!
//! Example:
//! ```rust,norun
//! use apalis_core::{monitor::Monitor, worker::Worker};
//!
//! let worker = WorkerBuilder::new("my-worker")
//!     .layer(my_middleware)
//!     .build(handler);
//! Monitor::new()
//!     .register(worker)
//!     .run()
//!     .await;
//! ```
//! [`Backend`]: crate::backend::Backend
//! [`ServiceFn`]: crate::service_fn::ServiceFn
//! [`Service`]: tower_service::Service
//! [`Request<Args, Context>`]: crate::request::Request
//! [`WorkerBuilder`]: crate::worker::builder::WorkerBuilder
//! [`Worker`]: crate::worker::Worker
//! [`Monitor`]: crate::monitor::Monitor
pub mod backend;
/// Includes internal error types.
pub mod error;
#[macro_use]
pub(crate) mod macros;
pub mod monitor;
pub mod request;
pub mod service_fn;
pub mod worker;

#[cfg(feature = "sleep")]
pub mod timer {
    pub use futures_timer::Delay;
    /// shorthand future for sleeping
    pub async fn sleep(duration: std::time::Duration) {
        futures_timer::Delay::new(duration).await;
    }
}

pub mod utils {
    pub use tower_layer::{Identity, Stack};
}
