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

use crate::{backend::Backend, task::Task, worker::context::WorkerContext};

pin_project_lite::pin_project! {
    #[must_use = "Custom backends must be polled or used as a sink"]
    pub struct CustomBackend<Args, DB, Fetch, Sink, IdType, Config = ()> {
        _marker: PhantomData<(Args, IdType)>,
        pool: DB,
        fetcher: Arc<Box<dyn Fn(&mut DB, &Config, &WorkerContext) -> Fetch>>,
        sinker: Arc<Box<dyn Fn(&mut DB, &Config) -> Sink>>,
        #[pin]
        current_sink: Sink,
        config: Config,
    }
}

impl<Args, DB, Fetch, Sink, IdType, Config> Clone
    for CustomBackend<Args, DB, Fetch, Sink, IdType, Config>
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

impl<Args, DB, Fetch, Sink, IdType: Clone, E, Meta: Default, Config> Backend<Args>
    for CustomBackend<Args, DB, Fetch, Sink, IdType, Config>
where
    Fetch: Stream<Item = Result<Option<Task<Args, Meta, IdType>>, E>>,
{
    type IdType = IdType;

    type Meta = Meta;

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

impl<Args, Meta, IdType, DB, Fetch, S, Config> Sink<Task<Args, Meta, IdType>>
    for CustomBackend<Args, DB, Fetch, S, IdType, Config>
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
        backend::{memory::MemoryStorage, TaskSink},
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
                        Some(item) => Some((Ok::<_, Infallible>(Some(item)), p)),
                        None => Some((Ok::<_, Infallible>(None), p)),
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
                        Ok::<_, Infallible>(p)
                    }
                    .boxed()
                })
            })
            .build()
            .unwrap();

        for i in 0..ITEMS {
            backend.push(i).await.unwrap();
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
