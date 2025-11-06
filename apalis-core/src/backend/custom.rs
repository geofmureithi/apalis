//! # Custom Backend
//!
//! A highly customizable backend for task processing that allows integration with any persistence engine by providing custom fetcher and sink functions.
//!
//! ## Overview
//!
//! The [`CustomBackend`] struct enables you to define how tasks are fetched from and persisted to
//! your storage engine.
//!
//! You can use the [`BackendBuilder`] to construct a [`CustomBackend`] by
//! providing the required database, fetcher, sink, and optional configuration and codec.
//!
//! ## Usage
//!
//! Use [`BackendBuilder`] to configure and build your custom backend:
//!
//! ## Example: CustomBackend with Worker
//!
//! ```rust
//! # use std::collections::VecDeque;
//! # use std::sync::Arc;
//! # use futures_util::{lock::Mutex, sink, stream};
//! # use apalis_core::backend::custom::{BackendBuilder, CustomBackend};
//! # use apalis_core::task::Task;
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_core::worker::context::WorkerContext;
//! # use apalis_core::error::BoxDynError;
//! # use std::time::Duration;
//! # use futures_util::StreamExt;
//! # use futures_util::FutureExt;
//! # use apalis_core::backend::TaskSink;
//! #[tokio::main]
//! async fn main() {
//!     // Create a memory-backed VecDeque
//!     let memory = Arc::new(Mutex::new(VecDeque::<Task<u32, ()>>::new()));
//!
//!     // Build the custom backend
//!     let mut backend = BackendBuilder::new()
//!         .database(memory)
//!         .fetcher(|memory, _, _| {
//!             stream::unfold(memory.clone(), |p| async move {
//!                 let mut memory = p.lock().await;
//!                 let item = memory.pop_front();
//!                 drop(memory);
//!                 match item {
//!                     Some(item) => Some((Ok::<_, BoxDynError>(Some(item)), p)),
//!                     None => Some((Ok::<_, BoxDynError>(None), p)),
//!                 }
//!             })
//!             .boxed()
//!         })
//!         .sink(|memory, _| {
//!             sink::unfold(memory.clone(), move |p, item| {
//!                 async move {
//!                     let mut memory = p.lock().await;
//!                     memory.push_back(item);
//!                     drop(memory);
//!                     Ok::<_, BoxDynError>(p)
//!                 }
//!                 .boxed()
//!             })
//!         })
//!         .build()
//!         .unwrap();
//!
//!     // Add a task to the backend
//!     backend.push(42).await.unwrap();
//!
//!     // Define the task handler
//!     async fn task(task: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
//!         tokio::time::sleep(Duration::from_secs(1)).await;
//! #       ctx.stop().unwrap();
//!         Ok(())
//!     }
//!
//!     // Build and run the worker
//!     let worker = WorkerBuilder::new("custom-worker")
//!         .backend(backend)
//!         .build(task);
//!     worker.run().await.unwrap();
//! }
//! ```
//!
//! ## Features
//!
//! - **Custom Fetcher**: Define how jobs are fetched from your storage.
//! - **Custom Sink**: Define how jobs are persisted to your storage.
//! - **Codec Support**: Optionally encode/decode task arguments.
//! - **Configurable**: Pass custom configuration to your backend.
//!
use futures_core::stream::BoxStream;
use futures_sink::Sink;
use futures_util::SinkExt;
use futures_util::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{fmt, marker::PhantomData};
use thiserror::Error;
use tower_layer::Identity;

use crate::backend::TaskStream;
use crate::backend::codec::Codec;
use crate::backend::codec::IdentityCodec;
use crate::error::BoxDynError;
use crate::features_table;
use crate::{backend::Backend, task::Task, worker::context::WorkerContext};

type Fetcher<DB, Config, Fetch> =
    Arc<Box<dyn Fn(&mut DB, &Config, &WorkerContext) -> Fetch + Send + Sync>>;

type Sinker<DB, Config, Sink> = Arc<Box<dyn Fn(&mut DB, &Config) -> Sink + Send + Sync>>;

/// A highly customizable backend for integration with any persistence engine
///
/// This backend allows you to define how tasks are fetched from and persisted to your storage,
/// meaning you can use it to integrate with existing systems.
///
/// # Example
/// ```rust,ignore
/// let backend = BackendBuilder::new()
///     .database(my_db)
///     .fetcher(my_fetcher_fn)
///     .sink(my_sink_fn)
///     .build()
///     .unwrap();
/// ```
#[doc = features_table! {
    setup = "{ unreachable!() }",
    TaskSink => supported("Ability to push new tasks", false),
    Serialization => supported("Serialization support for arguments", false),
    FetchById => not_supported("Allow fetching a task by its ID"),
    RegisterWorker => not_implemented("Allow registering a worker with the backend"),
    PipeExt => limited("Allow other backends to pipe to this backend", false), // Would require Clone,
    MakeShared => not_implemented("Share the same [`CustomBackend`] across multiple workers", false),
    Workflow => not_implemented("Flexible enough to support workflows"),
    WaitForCompletion => not_implemented("Wait for tasks to complete without blocking"), // Would require Clone
    ResumeById => not_supported("Resume a task by its ID"),
    ResumeAbandoned => not_supported("Resume abandoned tasks"),
    ListWorkers => not_implemented("List all workers registered with the backend"),
    ListTasks => not_implemented("List all tasks in the backend"),
}]
#[pin_project::pin_project]
#[must_use = "Custom backends must be polled or used as a sink"]
pub struct CustomBackend<Args, DB, Fetch, Sink, IdType, Codec = IdentityCodec, Config = ()> {
    _marker: PhantomData<(Args, IdType, Codec)>,
    db: DB,
    fetcher: Fetcher<DB, Config, Fetch>,
    sinker: Sinker<DB, Config, Sink>,
    #[pin]
    current_sink: Sink,
    config: Config,
}

impl<Args, DB, Fetch, Sink, IdType, Codec, Config> Clone
    for CustomBackend<Args, DB, Fetch, Sink, IdType, Codec, Config>
where
    DB: Clone,
    Config: Clone,
{
    fn clone(&self) -> Self {
        let mut db = self.db.clone();
        let current_sink = (self.sinker)(&mut db, &self.config);
        Self {
            _marker: PhantomData,
            db,
            fetcher: Arc::clone(&self.fetcher),
            sinker: Arc::clone(&self.sinker),
            current_sink,
            config: self.config.clone(),
        }
    }
}

impl<Args, DB, Fetch, Sink, IdType, Codec, Config> fmt::Debug
    for CustomBackend<Args, DB, Fetch, Sink, IdType, Codec, Config>
where
    DB: fmt::Debug,
    Config: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CustomBackend")
            .field(
                "_marker",
                &format_args!(
                    "PhantomData<({}, {})>",
                    std::any::type_name::<Args>(),
                    std::any::type_name::<IdType>()
                ),
            )
            .field("db", &self.db)
            .field("fetcher", &"Fn(&mut DB, &Config, &WorkerContext) -> Fetch")
            .field("sink", &"Fn(&mut DB, &Config) -> Sink")
            .field("config", &self.config)
            .finish()
    }
}

type FetcherBuilder<DB, Config, Fetch> =
    Box<dyn Fn(&mut DB, &Config, &WorkerContext) -> Fetch + Send + Sync + 'static>;

type SinkerBuilder<DB, Config, Sink> =
    Box<dyn Fn(&mut DB, &Config) -> Sink + Send + Sync + 'static>;

/// Builder for [`CustomBackend`]
///
/// Lets you set the database, fetcher, sink, codec, and config
pub struct BackendBuilder<Args, DB, Fetch, Sink, IdType, Codec = IdentityCodec, Config = ()> {
    _marker: PhantomData<(Args, IdType, Codec)>,
    database: Option<DB>,
    fetcher: Option<FetcherBuilder<DB, Config, Fetch>>,
    sink: Option<SinkerBuilder<DB, Config, Sink>>,
    config: Option<Config>,
}

impl<Args, DB, Fetch, Sink, IdType, Codec, Config> fmt::Debug
    for BackendBuilder<Args, DB, Fetch, Sink, IdType, Codec, Config>
where
    DB: fmt::Debug,
    Config: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackendBuilder")
            .field(
                "_marker",
                &format_args!(
                    "PhantomData<({}, {})>",
                    std::any::type_name::<Args>(),
                    std::any::type_name::<IdType>()
                ),
            )
            .field("database", &self.database)
            .field("fetcher", &self.fetcher.as_ref().map(|_| "Some(fn)"))
            .field("sink", &self.sink.as_ref().map(|_| "Some(fn)"))
            .field("config", &self.config)
            .finish()
    }
}

impl<Args, DB, Fetch, Sink, IdType, Codec, Config> Default
    for BackendBuilder<Args, DB, Fetch, Sink, IdType, Codec, Config>
{
    fn default() -> Self {
        Self {
            _marker: PhantomData,
            database: None,
            fetcher: None,
            sink: None,
            config: None,
        }
    }
}

impl<Args, DB, Fetch, Sink, IdType>
    BackendBuilder<Args, DB, Fetch, Sink, IdType, IdentityCodec, ()>
{
    /// Create a new `BackendBuilder` instance
    pub fn new() -> Self {
        Self::new_with_cfg(())
    }

    /// Create a new `BackendBuilder` instance with custom configuration
    pub fn new_with_cfg<Config>(
        config: Config,
    ) -> BackendBuilder<Args, DB, Fetch, Sink, IdType, IdentityCodec, Config> {
        BackendBuilder {
            config: Some(config),
            ..Default::default()
        }
    }
}

impl<Args, DB, Fetch, Sink, IdType, Codec, Config>
    BackendBuilder<Args, DB, Fetch, Sink, IdType, Codec, Config>
{
    /// Set a new codec for encoding/decoding task arguments
    pub fn with_codec<NewCodec>(
        self,
    ) -> BackendBuilder<Args, DB, Fetch, Sink, IdType, NewCodec, Config> {
        BackendBuilder {
            _marker: PhantomData,
            database: self.database,
            fetcher: self.fetcher,
            sink: self.sink,
            config: self.config,
        }
    }

    /// The custom backend persistence engine
    pub fn database(mut self, db: DB) -> Self {
        self.database = Some(db);
        self
    }

    /// The fetcher function to retrieve tasks from the database
    pub fn fetcher<F: Fn(&mut DB, &Config, &WorkerContext) -> Fetch + Send + Sync + 'static>(
        mut self,
        fetcher: F,
    ) -> Self {
        self.fetcher = Some(Box::new(fetcher));
        self
    }

    /// The sink function to persist tasks to the database
    pub fn sink<F: Fn(&mut DB, &Config) -> Sink + Send + Sync + 'static>(
        mut self,
        sink: F,
    ) -> Self {
        self.sink = Some(Box::new(sink));
        self
    }

    #[allow(clippy::type_complexity)]
    /// Build the `CustomBackend` instance
    pub fn build(
        self,
    ) -> Result<CustomBackend<Args, DB, Fetch, Sink, IdType, Codec, Config>, BuildError> {
        let mut db = self.database.ok_or(BuildError::MissingPool)?;
        let config = self.config.ok_or(BuildError::MissingConfig)?;
        let sink_fn = self.sink.ok_or(BuildError::MissingSink)?;
        let sink = sink_fn(&mut db, &config);

        Ok(CustomBackend {
            _marker: PhantomData,
            db,
            fetcher: self
                .fetcher
                .map(Arc::new)
                .ok_or(BuildError::MissingFetcher)?,
            current_sink: sink,
            sinker: Arc::new(sink_fn),
            config,
        })
    }
}

/// Errors encountered building a `CustomBackend`
#[derive(Debug, Error)]
pub enum BuildError {
    /// Missing database db
    #[error("Database db is required")]
    MissingPool,
    /// Missing fetcher function
    #[error("Fetcher is required")]
    MissingFetcher,
    /// Missing sink function
    #[error("Sink is required")]
    MissingSink,
    /// Missing configuration
    #[error("Config is required")]
    MissingConfig,
}

impl<Args, DB, Fetch, Sink, IdType: Clone, E, Ctx: Default, Encode, Config> Backend
    for CustomBackend<Args, DB, Fetch, Sink, IdType, Encode, Config>
where
    Fetch: Stream<Item = Result<Option<Task<Encode::Compact, Ctx, IdType>>, E>> + Send + 'static,
    Encode: Codec<Args> + Send + 'static,
    Encode::Error: Into<BoxDynError>,
    E: Into<BoxDynError>,
{
    type Args = Args;
    type IdType = IdType;

    type Context = Ctx;

    type Error = BoxDynError;

    type Stream = TaskStream<Task<Args, Ctx, IdType>, BoxDynError>;

    type Codec = Encode;

    type Compact = Encode::Compact;

    type Beat = BoxStream<'static, Result<(), Self::Error>>;

    type Layer = Identity;

    fn heartbeat(&self, _: &WorkerContext) -> Self::Beat {
        futures_util::stream::once(async { Ok(()) }).boxed()
    }

    fn middleware(&self) -> Self::Layer {
        Identity::new()
    }

    fn poll(mut self, worker: &WorkerContext) -> Self::Stream {
        (self.fetcher)(&mut self.db, &self.config, worker)
            .map(|task| match task {
                Ok(Some(t)) => Ok(Some(
                    t.try_map(|args| Encode::decode(&args))
                        .map_err(|e| e.into())?,
                )),
                Ok(None) => Ok(None),
                Err(e) => Err(e.into()),
            })
            .boxed()
    }
}

impl<Args, Ctx, IdType, DB, Fetch, S, Cdc, Config> Sink<Task<Cdc::Compact, Ctx, IdType>>
    for CustomBackend<Args, DB, Fetch, S, IdType, Cdc, Config>
where
    S: Sink<Task<Cdc::Compact, Ctx, IdType>>,
    Cdc: Codec<Args> + Send + 'static,
{
    type Error = S::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().current_sink.poll_ready_unpin(cx)
    }

    fn start_send(
        self: Pin<&mut Self>,
        item: Task<Cdc::Compact, Ctx, IdType>,
    ) -> Result<(), Self::Error> {
        self.project().current_sink.start_send_unpin(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().current_sink.poll_flush_unpin(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().current_sink.poll_close_unpin(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, time::Duration};

    use futures_util::{FutureExt, lock::Mutex, sink, stream};

    use crate::{
        backend::TaskSink,
        error::BoxDynError,
        worker::{builder::WorkerBuilder, ext::event_listener::EventListenerExt},
    };

    use super::*;

    const ITEMS: u32 = 10;

    #[tokio::test]
    async fn basic_custom_backend() {
        let memory: Arc<Mutex<VecDeque<Task<u32, ()>>>> = Arc::new(Mutex::new(VecDeque::new()));

        let mut backend = BackendBuilder::new()
            .database(memory)
            .fetcher(|db, _, _| {
                stream::unfold(db.clone(), |p| async move {
                    tokio::time::sleep(Duration::from_millis(100)).await; // Debounce
                    let mut db = p.lock().await;
                    let item = db.pop_front();
                    drop(db);
                    match item {
                        Some(item) => Some((Ok::<_, BoxDynError>(Some(item)), p)),
                        None => Some((Ok::<_, BoxDynError>(None), p)),
                    }
                })
                .boxed()
            })
            .sink(|db, _| {
                sink::unfold(db.clone(), move |p, item| {
                    async move {
                        let mut db = p.lock().await;
                        db.push_back(item);
                        drop(db);
                        Ok::<_, BoxDynError>(p)
                    }
                    .boxed()
                })
            })
            .build()
            .unwrap();

        for i in 0..ITEMS {
            TaskSink::push(&mut backend, i).await.unwrap();
        }

        async fn task(task: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task == ITEMS - 1 {
                ctx.stop().unwrap();
                return Err("Worker stopped!")?;
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(|ctx, ev| {
                println!("On Event = {:?} from {}", ev, ctx.name());
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
