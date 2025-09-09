//! Extension traits for building and extending workers
//!
//! Extension traits can be used for enhancing worker builder functionality that is not provided by default.
//!
//! # Available extension traits
//!
//! The available extensions include:
//!
//! - [`ack`]: Traits and utilities for acknowledging task completion.
//! - [`event_listener`]: Traits for subscribing to and handling worker events.
//! - [`long_running`]: Traits for building long running workers and tasks.
//! - [`circuit_breaker`]: Traits for adding circuit breaker patterns to workers.
//!
//! These extensions can be composed to customize worker behavior, improve reliability, and add observability.
//!
//! ## Creating a custom Worker extension trait
//!
//! The goal of using extension traits is to provide a way to add custom methods to the `WorkerBuilder` struct.
//! These methods can encapsulate common configurations or patterns that you want to reuse across different workers.
//! This example demonstrates how to define and implement an extension trait for `WorkerBuilder`.
//!
//! ```rust,ignore
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_core::backend::memory::MemoryStorage;
//! /// Example extension trait for WorkerBuilder that adds a custom method.
//! pub trait MakeSuperFastExt<Args, Ctx, Backend, Middleware>: Sized {
//!     /// Adds custom behavior to the WorkerBuilder.
//!     fn with_super_fast(self) -> WorkerBuilder<Args, Ctx, Backend, Middleware>;
//! }
//!
//! impl<Args, Ctx, Backend, Middleware> MakeSuperFastExt<Args, Ctx, Backend, Middleware>
//!     for WorkerBuilder<Args, Ctx, Backend, Middleware>
//! {
//!     fn with_super_fast(self) -> WorkerBuilder<Args, Ctx, Backend, Middleware> {
//!         // Insert your custom logic here
//!         // Do something with self, e.g., modify configuration, add middleware, etc.
//!         // The method can also accept parameters if needed, e.g., specific configuration options
//!         self
//!     }
//! }
//! #    async fn task(task: u32) {
//! #        println!("Processing task: {task}");
//! #    }
//! # let in_memory: MemoryStorage<()> = MemoryStorage::new();
//! // Usage
//! let builder = WorkerBuilder::new("my_worker")
//!     .backend(in_memory)
//!     .with_super_fast()
//!     .build(task);
//! ```
//! If your functionality is useful, consider contributing it as the `apalis-{my-functionality}-ext` crate and publishing it on crates.io.
pub mod ack;
pub mod circuit_breaker;
pub mod event_listener;
pub mod long_running;
