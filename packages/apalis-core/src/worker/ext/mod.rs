//! Extension traits for building workers
//!
//! This module provides extension traits and utilities for enhancing worker builder functionality.
//! The available extensions include:
//!
//! - [`ack`](crate::worker::ext::ack): Traits and utilities for acknowledging task completion.
//! - [`event_listener`](crate::worker::ext::event_listener): Traits for subscribing to and handling worker events.
//! - [`long_running`](crate::worker::ext::long_running): Traits for building long running workers and tasks.
//! - [`circuit_breaker`](crate::worker::ext::circuit_breaker): Traits for adding circuit breaker patterns to workers.
//!
//! These extensions can be composed to customize worker behavior, improve reliability, and add observability.
//!
//! ## Creating a custom Worker Extension trait
//!
//! The goal of using extension traits is to provide a way to add custom methods to the `WorkerBuilder` struct. 
//! These methods can encapsulate common configurations or patterns that you want to reuse across different workers.
//! This example demonstrates how to define and implement an extension trait for `WorkerBuilder`.
//!
//! ```rust
//! use apalis_core::worker::WorkerBuilder;
//!
//! /// Example extension trait for WorkerBuilder that adds a custom method.
//! pub trait MakeSuperFastExt<Args, Meta, Backend, Middleware>: Sized {
//!     /// Adds custom behavior to the WorkerBuilder.
//!     fn with_super_fast(self) -> WorkerBuilder<Args, Meta, Backend, Middleware>;
//! }
//!
//! impl<Args, Meta, Backend, Middleware> MakeSuperFastExt<Args, Meta, Backend, Middleware> for WorkerBuilder<Args, Meta, Backend, Middleware> {
//!     fn with_super_fast(self) -> WorkerBuilder<Args, Meta, Backend, Middleware> {
//!         // Insert your custom logic here
//!         // Do something with self, e.g., modify configuration, add middleware, etc.
//!         // The method can also accept parameters if needed eg specific configuration options
//!         self
//!     }
//! }
//!
//! // Usage
//! let builder = WorkerBuilder::new("my_worker")
//!     .backend(in_memory)
//!     .with_super_fast()
//!     .build(task);
//! ```
pub mod ack;
pub mod circuit_breaker;
pub mod event_listener;
pub mod long_running;
