use std::marker::PhantomData;
use std::sync::Arc;

use apalis_core::backend::{Backend, BackendWithSink};
use apalis_core::task::Task;
use apalis_core::utils::Identity;
use apalis_core::worker::context::WorkerContext;
use futures::stream::{self, BoxStream};
use futures::{Stream, StreamExt};

use crate::context::SqlContext;
use crate::Config;

#[derive(Clone)]
pub struct CustomBackend<Args, DB, Fetch, Sink> {
    _marker: PhantomData<Args>,
    pool: DB,
    fetcher: Arc<Box<dyn Fn(&mut DB, &Config, &WorkerContext) -> Fetch>>,
    sink: Arc<Box<dyn Fn(&mut DB, &Config) -> Sink>>,
}

/// Builder for `CustomSqlBackend`
pub struct BackendBuilder<Args, DB, Fetch, Sink> {
    _marker: PhantomData<Args>,
    database: Option<DB>,
    fetcher: Option<Box<dyn Fn(&mut DB, &Config, &WorkerContext) -> Fetch>>,
    sink: Option<Box<dyn Fn(&mut DB, &Config) -> Sink>>,
}

impl<Args, DB, Fetch, Sink> Default for BackendBuilder<Args, DB, Fetch, Sink> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
            database: None,
            fetcher: None,
            sink: None,
        }
    }
}

impl<Args, DB, Fetch, Sink> BackendBuilder<Args, DB, Fetch, Sink> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn database(mut self, db: DB) -> Self {
        self.database = Some(db);
        self
    }

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

    pub fn build(self) -> Result<CustomBackend<Args, DB, Fetch, Sink>, &'static str> {
        Ok(CustomBackend {
            _marker: PhantomData,
            pool: self.database.ok_or("Pool is required")?,
            fetcher: self.fetcher.map(Arc::new).ok_or("Fetcher is required")?,
            sink: self.sink.map(Arc::new).ok_or("Sink is required")?,
        })
    }
}

impl<Args, DB, Fetch, Sink, E> Backend<Args, SqlContext> for CustomBackend<Args, DB, Fetch, Sink>
where
    Fetch: Stream<Item = Result<Option<Task<Args, SqlContext>>, E>>,
{
    type Error = E;

    type Stream = Fetch;

    type Beat = BoxStream<'static, Result<(), Self::Error>>;

    type Layer = Identity;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        stream::once(async { Ok(()) }).boxed()
    }

    fn middleware(&self) -> Self::Layer {
        Identity::new()
    }

    fn poll(mut self, worker: &WorkerContext) -> Self::Stream {
        (self.fetcher)(&mut self.pool, &Config::default(), worker)
    }
}

impl<Args, DB, Fetch, Sink, E> BackendWithSink<Args, SqlContext>
    for CustomBackend<Args, DB, Fetch, Sink>
where
    Fetch: Stream<Item = Result<Option<Task<Args, SqlContext>>, E>>,
    Sink: futures::Sink<Task<Args, SqlContext>>,
{
    type Sink = Sink;

    fn sink(&mut self) -> Self::Sink {
        (self.sink)(&mut self.pool, &Config::default())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::ready,
        ops::Deref,
        str::FromStr,
        sync::{atomic::AtomicUsize, Arc},
        time::Duration,
    };

    use apalis_core::{
        backend::{codec::json::JsonCodec, memory::MemoryStorage, BackendWithSink, TaskSink},
        error::BoxDynError,
        service_fn::{self, service_fn, ServiceFn},
        task::{task_id::TaskId, Metadata},
        worker::{
            builder::WorkerBuilder,
            context::WorkerContext,
            ext::{
                ack::{Acknowledge, AcknowledgeLayer, AcknowledgeService, AcknowledgementExt},
                circuit_breaker::CircuitBreaker,
                event_listener::EventListenerExt,
                long_running::LongRunningExt,
            },
        },
    };
    use chrono::{DateTime, Utc};
    use futures::{sink, FutureExt};
    use serde_json::Value;
    use sqlx::PgPool;
    use tower::limit::ConcurrencyLimitLayer;

    use crate::postgres::{fetcher::PgFetcher, PgAck, PgSink};

    use super::*;

    const ITEMS: u32 = 10;

    #[tokio::test]
    async fn basic_custom_backend() {
        let pool = PgPool::connect("postgres://postgres:postgres@localhost/apalis_dev")
            .await
            .unwrap();
        let mut backend = BackendBuilder::new()
            .database(pool.clone())
            .fetcher(|pool, config, wrk| {
                PgFetcher::<_, _, JsonCodec<Value>>::new(pool, config, wrk)
            })
            .sink(|pool, config| PgSink::<_, _, JsonCodec<Value>>::new(pool, config))
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
            .layer(AcknowledgeLayer::new(PgAck::new(pool)))
            .break_circuit()
            .long_running()
            .on_event(|ctx, ev| {
                println!("On Event = {:?}", ev);
            })
            .build(task);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn queries_custom_backend() {
        #[derive(Debug, Clone)]
        struct Email {
            to: String,
            subject: String,
            message: String,
        }
        let pool = PgPool::connect("postgres://postgres:postgres@localhost/apalis_dev")
            .await
            .unwrap();
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS emails  (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'Pending', -- Pending, Sending, Done, Failed
            attempts INTEGER NOT NULL DEFAULT 0,    -- number of send attempts
            recipient TEXT NOT NULL,                       -- recipient address
            subject TEXT NOT NULL,
            message TEXT NOT NULL,
            send_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- scheduled send time
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        let mut backend = BackendBuilder::new()
            .database(pool.clone())
            .fetcher(|pool, config, wrk| {
                stream::unfold(pool.clone(), |pool| {
                    let p = pool.clone();
                    let fetch = async move {
                        println!("Called");
                        let mut tx = p.begin().await?;
                        let res = sqlx::query(r#"
                            WITH next_email AS (
                                SELECT id
                                FROM emails
                                WHERE status = 'Pending'
                                AND send_at <= NOW()
                                ORDER BY send_at ASC, id ASC
                                LIMIT 1
                                FOR UPDATE SKIP LOCKED
                            )
                            UPDATE emails
                            SET status = 'Sending',
                                updated_at = NOW(),
                                attempts = attempts + 1
                            FROM next_email
                            WHERE emails.id = next_email.id
                            RETURNING emails.*;
                        "#)
                            .fetch_all(&p)
                            .await?;
                        tx.commit().await?;
                        Ok::<_, sqlx::Error>(res)
                    };
                    async move {
                        let rows = fetch.await.unwrap();
                        Some((rows, pool))
                    }
                    .boxed()
                }).flat_map(|rows| stream::iter(rows))
                .map(|row| { //TODO: Make clone
                     use sqlx::Row;
                    let email = Email {
                        to: row.try_get("recipient")?,
                        message: row.try_get("message")?,
                        subject: row.try_get("subject")?
                    };
                    let send_at: DateTime<Utc> = row.get("send_at");
                    let t =Task::builder(email)
                        .with_task_id(TaskId::from_str(row.try_get("id")?).map_err(|e| sqlx::Error::Decode(e.into()))?)
                        .run_at_timestamp(send_at.timestamp() as u64)
                        .build();
                    Ok::<_, sqlx::Error>(Some(t))
                })
            })
            .sink(|pool, config| {
                let sink = sink::unfold(pool.clone(), |pool, item: Task<Email, SqlContext>| {
                    let p = pool.clone();
                    async move {
                        let mut tx = p.begin().await?;
                        sqlx::query("INSERT INTO emails (id, recipient, subject, message, send_at) VALUES ($1, $2, $3, $4, $5)")
                            .bind(&item.meta.task_id.to_string())
                            .bind(&item.args.to)
                            .bind(&item.args.subject)
                            .bind(&item.args.message)
                            .bind(&DateTime::from_timestamp(item.meta.run_at as i64, 0))
                            .execute(&mut *tx)
                            .await?;
                        tx.commit().await?;
                        Ok::<_, sqlx::Error>(pool)
                    }
                    .boxed()
                });
                sink
            })
            .build()
            .unwrap();
        let mut sink = backend.sink();
        for i in 0..ITEMS {
            sink.push(Email {
                to: "one@gmail.com".to_owned(),
                subject: i.to_string(),
                message: "Not spam".to_owned(),
            })
            .await
            .unwrap();
        }

        async fn task(task: Email, ctx: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if task.subject == (ITEMS - 1).to_string() {
                ctx.stop().unwrap();
                return Err("Worker stopped!")?;
            }
            Ok(())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .layer(ConcurrencyLimitLayer::new(1))
            .ack_with(
                move |res: &Result<(), BoxDynError>, meta: &Metadata<SqlContext>| {
                    let p = pool.clone();
                    let id = meta.task_id.to_string();
                    let status = match res {
                        Ok(_) => "Done",
                        Err(_) => "Failed",
                    };
                    async move {
                        let mut tx = p.begin().await.unwrap();
                        sqlx::query(
                            "UPDATE emails SET status = $2, updated_at = NOW() WHERE id = $1;",
                        )
                        .bind(id)
                        .bind(status)
                        .execute(&mut *tx)
                        .await?;
                        tx.commit().await?;
                        Ok::<_, sqlx::Error>(())
                    }
                },
            )
            .on_event(|ctx, ev| {
                println!("On Event = {:?}", ev);
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
