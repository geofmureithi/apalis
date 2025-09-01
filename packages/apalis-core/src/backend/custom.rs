//! # Custom Backend
//!
//! This module provides a highly customizable backend for `apalis` task processing,
//! allowing integration with any persistence engine by providing custom fetcher and sink functions.
//!
//! ## Overview
//!
//! The [`CustomBackend`] struct enables you to define how tasks are fetched from and persisted to
//! your storage engine. You can use the [`BackendBuilder`] to construct a `CustomBackend` by
//! providing the required database, fetcher, sink, and optional configuration and codec.
//!
//! ## Usage
//!
//! Use [`BackendBuilder`] to configure and build your custom backend:
//!
//! ```rust
//! # use std::collections::VecDeque;
//! # use std::sync::Arc;
//! # use futures_util::{lock::Mutex, sink, stream};
//! # use apalis_core::backend::custom::{BackendBuilder, CustomBackend};
//! # use apalis_core::task::Task;
//! let memory: Arc<Mutex<VecDeque<Task<u32, ()>>>> = Arc::new(Mutex::new(VecDeque::new()));
//!
//! let backend = BackendBuilder::new()
//!     .database(memory.clone())
//!     .fetcher(|pool, _, _| {
//!         stream::unfold(pool.clone(), |p| async move {
//!             let mut pool = p.lock().await;
//!             let item = pool.pop_front();
//! #            drop(pool);
//!             match item {
//!                 Some(item) => Some((Ok::<_, BoxDynError>(Some(item)), p)),
//!                 None => Some((Ok::<_, BoxDynError(None), p)),
//!             }
//!         })
//!         .boxed()
//!     })
//!     .sink(|pool, _| {
//!         sink::unfold(pool.clone(), move |p, item| {
//!             async move {
//!                 let mut pool = p.lock().await;
//!                 pool.push_back(item);
//! #                drop(pool);
//!                 Ok::<_, BoxDynError>(p)
//!             }
//!             .boxed()
//!         })
//!     })
//!     .build()
//!     .unwrap();
//! ```
//!
//! ## Example: Using with a Worker
//!
//! ```rust
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_core::worker::context::WorkerContext;
//! # use apalis_core::error::BoxDynError;
//! # use std::time::Duration;
//! async fn task(_: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
//!     tokio::time::sleep(Duration::from_secs(1)).await;
//! #   ctx.stop().unwrap();
//!     Ok(())
//! }
//! 
//! backend.push(42).await.unwrap(); // Add a task to the backend
//!
//! let worker = WorkerBuilder::new("custom-worker")
//!     .backend(backend)
//!     .build(task);
//! worker.run().await.unwrap();
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

use crate::backend::codec::Codec;
use crate::backend::codec::IdentityCodec;
use crate::backend::TaskStream;
use crate::error::BoxDynError;
use crate::{backend::Backend, task::Task, worker::context::WorkerContext};

pin_project_lite::pin_project! {
    /// A highly customizable backend for apalis, allowing integration with any persistence engine
    /// by providing custom fetcher and sink functions.
    /// 
    /// # Usage
    /// Use [`BackendBuilder`] to construct a `CustomBackend` by providing the required
    /// database, fetcher, sink, and optional configuration and codec.
    ///
    /// # Example
    /// ```rust
    /// let backend = BackendBuilder::new()
    ///     .database(my_db)
    ///     .fetcher(my_fetcher_fn)
    ///     .sink(my_sink_fn)
    ///     .build()
    ///     .unwrap();
    /// ```
    #[must_use = "Custom backends must be polled or used as a sink"]
    pub struct CustomBackend<Args, DB, Fetch, Sink, IdType, Codec = IdentityCodec, Config = ()> {
        _marker: PhantomData<(Args, IdType, Codec)>,
        pool: DB,
        fetcher: Arc<Box<dyn Fn(&mut DB, &Config, &WorkerContext) -> Fetch + Send + Sync>>,
        sinker: Arc<Box<dyn Fn(&mut DB, &Config) -> Sink + Send + Sync>>,
        #[pin]
        current_sink: Sink,
        config: Config,
    }
}

impl<Args, DB, Fetch, Sink, IdType, Codec, Config> Clone
    for CustomBackend<Args, DB, Fetch, Sink, IdType, Codec, Config>
where
    DB: Clone,
    Config: Clone,
{
    fn clone(&self) -> Self {
        let mut pool = self.pool.clone();
        let current_sink = (self.sinker)(&mut pool, &self.config);
        Self {
            _marker: PhantomData,
            pool,
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
            .field("pool", &self.pool)
            .field("fetcher", &"Fn(&mut DB, &Config, &WorkerContext) -> Fetch")
            .field("sink", &"Fn(&mut DB, &Config) -> Sink")
            .field("config", &self.config)
            .finish()
    }
}

/// Builder for `CustomBackend`
/// Allows setting the database, fetcher, sink, codec, and configuration
pub struct BackendBuilder<Args, DB, Fetch, Sink, IdType, Codec = IdentityCodec, Config = ()> {
    _marker: PhantomData<(Args, IdType, Codec)>,
    database: Option<DB>,
    fetcher: Option<Box<dyn Fn(&mut DB, &Config, &WorkerContext) -> Fetch + Send + Sync + 'static>>,
    sink: Option<Box<dyn Fn(&mut DB, &Config) -> Sink + Send + Sync + 'static>>,
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
}

impl<Args, DB, Fetch, Sink, IdType, Codec, Config>
    BackendBuilder<Args, DB, Fetch, Sink, IdType, Codec, Config>
{
    /// Create a new `BackendBuilder` instance with custom configuration
    pub fn new_with_cfg(config: Config) -> Self {
        Self {
            config: Some(config),
            ..Default::default()
        }
    }

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

    /// Build the `CustomBackend` instance
    pub fn build(
        self,
    ) -> Result<CustomBackend<Args, DB, Fetch, Sink, IdType, Codec, Config>, BuildError> {
        let mut pool = self.database.ok_or(BuildError::MissingPool)?;
        let config = self.config.ok_or(BuildError::MissingConfig)?;
        let sink_fn = self.sink.ok_or(BuildError::MissingSink)?;
        let sink = sink_fn(&mut pool, &config);

        Ok(CustomBackend {
            _marker: PhantomData,
            pool,
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

/// Errors that can occur when building a `CustomBackend`
#[derive(Debug, Error)]
pub enum BuildError {
    /// Missing database pool
    #[error("Database pool is required")]
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

impl<Args, DB, Fetch, Sink, IdType: Clone, E, Meta: Default, Encode, Config> Backend<Args>
    for CustomBackend<Args, DB, Fetch, Sink, IdType, Encode, Config>
where
    Fetch: Stream<Item = Result<Option<Task<Encode::Compact, Meta, IdType>>, E>> + Send + 'static,
    Encode: Codec<Args> + Send + 'static,
    Encode::Error: Into<BoxDynError>,
    E: Into<BoxDynError>,
{
    type IdType = IdType;

    type Meta = Meta;

    type Error = BoxDynError;

    type Stream = TaskStream<Task<Args, Meta, IdType>, BoxDynError>;

    type Codec = Encode;

    type Beat = BoxStream<'static, Result<(), Self::Error>>;

    type Layer = Identity;

    fn heartbeat(&self, _: &WorkerContext) -> Self::Beat {
        futures_util::stream::once(async { Ok(()) }).boxed()
    }

    fn middleware(&self) -> Self::Layer {
        Identity::new()
    }

    fn poll(mut self, worker: &WorkerContext) -> Self::Stream {
        (self.fetcher)(&mut self.pool, &self.config, worker)
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

impl<Args, Meta, IdType, DB, Fetch, S, Codec, Config> Sink<Task<Args, Meta, IdType>>
    for CustomBackend<Args, DB, Fetch, S, IdType, Codec, Config>
where
    S: Sink<Task<Args, Meta, IdType>>,
{
    type Error = S::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().current_sink.poll_ready_unpin(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Task<Args, Meta, IdType>) -> Result<(), Self::Error> {
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
    use std::{collections::VecDeque, convert::Infallible, time::Duration};

    use futures_channel::mpsc::SendError;
    use futures_util::{lock::Mutex, sink, stream, FutureExt};
    use tokio::sync::RwLock;
    use tower::limit::ConcurrencyLimitLayer;

    use crate::{
        backend::{impls::memory::MemoryStorage, TaskSink},
        error::BoxDynError,
        task::{builder::TaskBuilder, task_id::RandomId},
        worker::{builder::WorkerBuilder, ext::event_listener::EventListenerExt},
    };

    use super::*;

    const ITEMS: u32 = 10;

    #[tokio::test]
    async fn basic_custom_backend() {
        let memory: Arc<Mutex<VecDeque<Task<u32, ()>>>> = Arc::new(Mutex::new(VecDeque::new()));

        let mut backend = BackendBuilder::new()
            .database(memory)
            .fetcher(|pool, _, _| {
                stream::unfold(pool.clone(), |p| async move {
                    tokio::time::sleep(Duration::from_millis(100)).await; // Debounce
                    let mut pool = p.lock().await;
                    let item = pool.pop_front();
                    drop(pool);
                    match item {
                        Some(item) => Some((Ok::<_, BoxDynError>(Some(item)), p)),
                        None => Some((Ok::<_, BoxDynError>(None), p)),
                    }
                })
                .boxed()
            })
            .sink(|pool, _| {
                sink::unfold(pool.clone(), move |p, item| {
                    async move {
                        let mut pool = p.lock().await;
                        pool.push_back(item);
                        drop(pool);
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
                println!("On Event = {:?}", ev);
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
