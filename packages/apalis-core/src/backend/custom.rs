use futures_core::stream::BoxStream;
use futures_util::{Stream, StreamExt};
use std::sync::Arc;
use std::{fmt, marker::PhantomData};
use tower_layer::Identity;
use thiserror::Error;

use crate::{
    backend::{Backend, BackendWithSink},
    task::Task,
    worker::context::WorkerContext,
};

#[derive(Clone)]
pub struct CustomBackend<Args, DB, Fetch, Sink, IdType, Config = ()> {
    _marker: PhantomData<(Args, IdType)>,
    pool: DB,
    fetcher: Arc<Box<dyn Fn(&mut DB, &Config, &WorkerContext) -> Fetch>>,
    sink: Arc<Box<dyn Fn(&mut DB, &Config) -> Sink>>,
    config: Config,
}

impl<Args, DB, Fetch, Sink, IdType, Config> fmt::Debug
    for CustomBackend<Args, DB, Fetch, Sink, IdType, Config>
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

/// Builder for `CustomSqlBackend`
pub struct BackendBuilder<Args, DB, Fetch, Sink, IdType, Config = ()> {
    _marker: PhantomData<(Args, IdType)>,
    database: Option<DB>,
    fetcher: Option<Box<dyn Fn(&mut DB, &Config, &WorkerContext) -> Fetch + 'static>>,
    sink: Option<Box<dyn Fn(&mut DB, &Config) -> Sink + 'static>>,
    config: Option<Config>,
}

impl<Args, DB, Fetch, Sink, IdType, Config> fmt::Debug
    for BackendBuilder<Args, DB, Fetch, Sink, IdType, Config>
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

impl<Args, DB, Fetch, Sink, IdType, Config> Default
    for BackendBuilder<Args, DB, Fetch, Sink, IdType, Config>
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

impl<Args, DB, Fetch, Sink, IdType> BackendBuilder<Args, DB, Fetch, Sink, IdType, ()> {
    pub fn new() -> Self {
        Self::new_with_cfg(())
    }
}

impl<Args, DB, Fetch, Sink, IdType, Config> BackendBuilder<Args, DB, Fetch, Sink, IdType, Config> {
    pub fn new_with_cfg(config: Config) -> Self {
        Self {
            config: Some(config),
            ..Default::default()
        }
    }

    /// The custom backend persistence engine
    pub fn database(mut self, db: DB) -> Self {
        self.database = Some(db);
        self
    }

    /// The fetcher
    pub fn fetcher<F: Fn(&mut DB, &Config, &WorkerContext) -> Fetch + 'static>(
        mut self,
        fetcher: F,
    ) -> Self {
        self.fetcher = Some(Box::new(fetcher));
        self
    }

    pub fn sink<F: Fn(&mut DB, &Config) -> Sink + 'static>(mut self, sink: F) -> Self {
        self.sink = Some(Box::new(sink));
        self
    }

    pub fn build(self) -> Result<CustomBackend<Args, DB, Fetch, Sink, IdType, Config>, BuildError> {
        Ok(CustomBackend {
            _marker: PhantomData,
            pool: self.database.ok_or(BuildError::MissingPool)?,
            fetcher: self
                .fetcher
                .map(Arc::new)
                .ok_or(BuildError::MissingFetcher)?,
            sink: self.sink.map(Arc::new).ok_or(BuildError::MissingSink)?,
            config: self.config.ok_or(BuildError::MissingConfig)?,
        })
    }
}

#[derive(Debug, Error)]
pub enum BuildError {
    #[error("Database pool is required")]
    MissingPool,
    #[error("Fetcher is required")]
    MissingFetcher,
    #[error("Sink is required")]
    MissingSink,
    #[error("Config is required")]
    MissingConfig,
}

impl<Args, DB, Fetch, Sink, IdType, E, Meta, Config> Backend<Args, Meta>
    for CustomBackend<Args, DB, Fetch, Sink, IdType, Config>
where
    Fetch: Stream<Item = Result<Option<Task<Args, Meta, IdType>>, E>>,
{
    type IdType = IdType;

    type Error = E;

    type Stream = Fetch;

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
    }
}

impl<Args, DB, Fetch, Sink, E, IdType, Meta> BackendWithSink<Args, Meta>
    for CustomBackend<Args, DB, Fetch, Sink, IdType>
where
    Fetch: Stream<Item = Result<Option<Task<Args, Meta, IdType>>, E>>,
    Sink: futures_sink::Sink<Task<Args, Meta, IdType>>,
    CustomBackend<Args, DB, Fetch, Sink, IdType>: Backend<Args, Meta, IdType = IdType>,
{
    type Sink = Sink;

    fn sink(&mut self) -> Self::Sink {
        (self.sink)(&mut self.pool, &self.config)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{
        backend::{memory::MemoryStorage, TaskSink},
        error::BoxDynError,
        worker::{builder::WorkerBuilder, ext::event_listener::EventListenerExt},
    };

    use super::*;

    const ITEMS: u32 = 10;

    #[tokio::test]
    async fn basic_custom_backend() {
        let memory = MemoryStorage::new_with_json();

        let mut backend = BackendBuilder::new()
            .database(memory)
            .fetcher(|pool, _, worker| pool.clone().poll(worker))
            .sink(|pool, _| pool.sink())
            .build()
            .unwrap();
        let mut sink = backend.sink();
        for i in 0..ITEMS {
            sink.push(i).await.unwrap();
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
