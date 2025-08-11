use std::{
    collections::{HashMap, HashSet},
    future::ready,
    marker::PhantomData,
    sync::Arc,
};

use apalis_core::{
    backend::{codec::json::JsonCodec, shared::MakeShared},
    request::task_id::TaskId,
    worker::context::WorkerContext,
};
use futures::{
    channel::mpsc::{self, Receiver, Sender},
    future::{self, BoxFuture},
    lock::Mutex,
    FutureExt, SinkExt, StreamExt, TryStreamExt,
};
use serde::Deserialize;
use sqlx::{postgres::PgListener, PgPool};

use crate::{
    postgres::{fetcher::PgFetcher, PgTask, PostgresStorage},
    Config,
};

pub struct SharedPostgresStorage {
    pool: PgPool,
    registry: Arc<Mutex<HashMap<String, Sender<TaskId>>>>,
    drive: BoxFuture<'static, ()>,
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

                let notification = listener
                    .into_stream()
                    .try_filter(|notification| {
                        let payload = notification.payload().to_owned();
                        let instances = instances.clone();
                        async move {
                            let instances = instances.lock().await;
                            let mut keys = instances.keys();
                            keys.find(|key| payload.starts_with(*key)).is_some()
                        }
                    })
                    .filter_map(|s| future::ready(s.ok()))
                    .map(|notification| {
                        let payload = notification.payload();
                        let ev: InsertEvent = serde_json::from_str(payload).unwrap();
                        ev
                    })
                    .then(|ev| {
                        let instances = instances.clone();
                        async move {
                            let mut instances = instances.lock().await;
                            let sender = instances.get_mut(&ev.job_type).unwrap();
                            sender.send(ev.id).await.unwrap();
                        }
                    });
            }
            .boxed(),
            registry,
        }
    }
}

impl<Args> MakeShared<Args> for SharedPostgresStorage {
    type Backend = PostgresStorage<Args, Receiver<TaskId>>;
    type Config = Config;
    type MakeError = ();
    fn make_shared_with_config(
        &mut self,
        config: Self::Config,
    ) -> Result<PostgresStorage<Args, Receiver<TaskId>>, Self::MakeError> {
        let (tx, rx) = mpsc::channel(config.buffer_size);
        let mut r = self.registry.try_lock().unwrap();
        r.insert(config.namespace().to_owned(), tx);
        Ok(PostgresStorage {
            _marker: PhantomData,
            config,
            fetcher: Some(rx),
            pool: self.pool.clone(),
        })
    }
}
