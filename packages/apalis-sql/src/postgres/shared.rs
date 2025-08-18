use std::{
    collections::{HashMap, HashSet},
    future::ready,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use apalis_core::{
    backend::{
        codec::{json::JsonCodec, Decoder},
        shared::MakeShared,
        Backend, BackendWithSink, TaskStream,
    },
    task::{
        task_id::{TaskId, Ulid},
        Task,
    },
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use chrono::Utc;
use futures::{
    channel::mpsc::{self, Receiver, Sender},
    future::{self, BoxFuture, Shared},
    lock::Mutex,
    stream::{self, select, BoxStream},
    FutureExt, SinkExt, Stream, StreamExt, TryStreamExt,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use sqlx::{postgres::PgListener, PgPool};

use crate::{
    context::SqlContext,
    postgres::{fetcher::PgFetcher, register, CompactT, PgAck, PgSink, PgTask, PostgresStorage},
    Config,
};

pub struct SharedPostgresStorage<Compact = Value, Codec = JsonCodec<Value>> {
    pool: PgPool,
    registry: Arc<Mutex<HashMap<String, Sender<TaskId>>>>,
    drive: Shared<BoxFuture<'static, ()>>,
    _marker: PhantomData<(Compact, Codec)>,
}

#[derive(Debug, Deserialize)]
struct InsertEvent {
    job_type: String,
    id: TaskId,
}

impl SharedPostgresStorage {
    pub fn new(pool: PgPool) -> Self {
        let registry: Arc<Mutex<HashMap<String, Sender<TaskId>>>> =
            Arc::new(Mutex::new(HashMap::default()));
        let p = pool.clone();
        let instances = registry.clone();
        Self {
            pool,
            drive: async move {
                let mut listener = PgListener::connect_with(&p).await.unwrap();
                listener.listen("apalis::job::insert").await.unwrap();
                listener
                    .into_stream()
                    .filter_map(|notification| {
                        let instances = instances.clone();
                        async move {
                            let pg_notification = notification.ok()?;
                            let payload = pg_notification.payload();
                            let ev: InsertEvent = serde_json::from_str(payload).ok()?;
                            let instances = instances.lock().await;
                            let mut keys = instances.keys();
                            if keys.find(|key| &ev.job_type == *key).is_some() {
                                return Some(ev);
                            }
                            None
                        }
                    })
                    .for_each(|ev| {
                        let instances = instances.clone();
                        async move {
                            let mut instances = instances.lock().await;
                            let sender = instances.get_mut(&ev.job_type).unwrap();
                            sender.send(ev.id).await.unwrap();
                        }
                    })
                    .await;
            }
            .boxed()
            .shared(),
            registry,
            _marker: PhantomData,
        }
    }
}
#[derive(Debug)]
pub enum SharedPostgresError {
    NamespaceExists(String),
    RegistryLocked,
}

impl<Args, Compact, Codec> MakeShared<Args> for SharedPostgresStorage<Compact, Codec> {
    type Backend = PostgresStorage<Args, Compact, Codec, SharedFetcher>;
    type Config = Config;
    type MakeError = SharedPostgresError;
    fn make_shared(&mut self) -> Result<Self::Backend, Self::MakeError>
    where
        Self::Config: Default,
    {
        Self::make_shared_with_config(self, Config::new(std::any::type_name::<Args>()))
    }
    fn make_shared_with_config(
        &mut self,
        config: Self::Config,
    ) -> Result<Self::Backend, Self::MakeError> {
        let (tx, rx) = mpsc::channel(config.buffer_size);
        let mut r = self
            .registry
            .try_lock()
            .ok_or(SharedPostgresError::RegistryLocked)?;
        if let Some(_) = r.insert(config.namespace().to_owned(), tx) {
            return Err(SharedPostgresError::NamespaceExists(
                config.namespace().to_owned(),
            ));
        }
        Ok(PostgresStorage {
            _marker: PhantomData,
            config,
            fetcher: SharedFetcher {
                poller: self.drive.clone(),
                receiver: rx,
            },
            pool: self.pool.clone(),
        })
    }
}

pub struct SharedFetcher {
    poller: Shared<BoxFuture<'static, ()>>,
    receiver: Receiver<TaskId>,
}

impl Stream for SharedFetcher {
    type Item = TaskId;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Keep the poller alive by polling it, but ignoring the output
        let _ = this.poller.poll_unpin(cx);

        // Delegate actual items to receiver
        this.receiver.poll_next_unpin(cx)
    }
}

impl<Args, Decode> Backend<Args, SqlContext>
    for PostgresStorage<Args, CompactT, Decode, SharedFetcher>
where
    Args: Send + 'static + Unpin,
    Decode: Decoder<Args, Compact = CompactT> + 'static + Unpin + Send,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type IdType = Ulid;

    type Error = sqlx::Error;

    type Stream = TaskStream<Task<Args, SqlContext, Ulid>, sqlx::Error>;

    type Beat = BoxStream<'static, Result<(), sqlx::Error>>;

    type Layer = AcknowledgeLayer<PgAck>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let worker_type = self.config.namespace().to_owned();
        let fut = register(
            self.pool.clone(),
            worker_type,
            worker.clone(),
            Utc::now().timestamp(),
        );
        stream::once(fut).boxed()
    }

    fn middleware(&self) -> Self::Layer {
        AcknowledgeLayer::new(PgAck {
            pool: self.pool.clone(),
        })
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        let pool = self.pool.clone();
        let worker_id = worker.name().to_owned();
        let lazy_fetcher = self
            .fetcher
            .map(|t| t.to_string())
            .ready_chunks(self.config.buffer_size)
            .then(move |ids| {
                let pool = pool.clone();
                let worker_id = worker_id.clone();
                async move {
                    let mut tx = pool.begin().await?;
                    let res: Vec<_> = sqlx::query_as!(
                        PgTask,
                        r#"
UPDATE apalis.jobs
SET status = 'Running',
    lock_at = now(),
    lock_by = $2
WHERE id IN (
        SELECT id
        FROM apalis.jobs
        WHERE (status='Pending' OR (status = 'Failed' AND attempts < max_attempts))
            AND run_at < now()
            AND id = ANY($1)
        ORDER BY run_at ASC
        FOR UPDATE skip LOCKED
    )
returning *;"#,
                        &ids,
                        &worker_id
                    )
                    .fetch(&mut *tx)
                    .map(|r| Ok(Some(r?.try_into_req::<Decode, Args>()?)))
                    .collect()
                    .await;
                    tx.commit().await?;
                    Ok::<_, sqlx::Error>(res)
                }
            })
            .flat_map(|vec| match vec {
                Ok(vec) => stream::iter(vec.into_iter().map(|res| match res {
                    Ok(t) => Ok(t),
                    Err(e) => Err(e),
                }))
                .boxed(),
                Err(e) => stream::once(ready(Err(e))).boxed(),
            })
            .boxed();

        let eager_fetcher = StreamExt::boxed(PgFetcher::<Args, CompactT, Decode>::new(
            &self.pool,
            &self.config,
            worker,
        ));
        select(lazy_fetcher, eager_fetcher).boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use chrono::Local;

    use apalis_core::{
        backend::{memory::MemoryStorage, TaskSink},
        error::BoxDynError,
        worker::{builder::WorkerBuilder, event::Event, ext::event_listener::EventListenerExt},
    };
    use tower::limit::ConcurrencyLimitLayer;

    use super::*;

    #[tokio::test]
    async fn basic_worker() {
        let pool = PgPool::connect("postgres://postgres:postgres@localhost/apalis_dev")
            .await
            .unwrap();
        let mut store = SharedPostgresStorage::new(pool);

        let mut map_store = store.make_shared().unwrap();

        let mut int_store = store.make_shared().unwrap();

        let mut int_sink = int_store.sink();

        let mut map_sink = map_store.sink();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            let task = Task::builder(99u32)
                .run_after(Duration::from_secs(2))
                .with_context(|mut ctx: SqlContext| {
                    ctx.set_priority(1);
                    ctx
                })
                .build();
            let task2 = Task::builder(Default::default())
                .with_context(|mut ctx: SqlContext| {
                    ctx.set_priority(2);
                    ctx
                })
                // .run_after(Duration::from_secs(5))
                .build();

            map_sink
                .send_all(&mut stream::iter(vec![task, task2].into_iter().map(Ok)))
                .await
                .unwrap();
            int_sink.push(99).await.unwrap();
        });

        async fn send_reminder<T, I>(
            _: T,
            ctx: SqlContext,
            task_id: TaskId<I>,
        ) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(2)).await;
            // Err("Failed".into())
            Ok(())
        }

        let int_worker = WorkerBuilder::new("rango-tango-2")
            .backend(int_store)
            .layer(ConcurrencyLimitLayer::new(1))
            .on_event(move |ctx, ev| {
                println!("{:?}", ev);
                let ctx = ctx.clone();

                if matches!(ev, Event::Start) {
                    tokio::spawn(async move {
                        if ctx.is_running() {
                            tokio::time::sleep(Duration::from_millis(15000)).await;
                            ctx.stop().unwrap();
                        }
                    });
                }
            })
            .build(send_reminder);
        let map_worker = WorkerBuilder::new("rango-tango-1")
            .backend(map_store)
            .layer(ConcurrencyLimitLayer::new(1))
            .on_event(move |ctx, ev| {
                println!("{:?}", ev);
                let ctx = ctx.clone();

                if matches!(ev, Event::Start) {
                    tokio::spawn(async move {
                        if ctx.is_running() {
                            tokio::time::sleep(Duration::from_millis(15000)).await;
                            ctx.stop().unwrap();
                        }
                    });
                }
            })
            .build(send_reminder);
        let res = tokio::try_join!(int_worker.run(), map_worker.run()).unwrap();
    }
}
