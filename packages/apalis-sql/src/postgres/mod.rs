use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    panic,
    pin::Pin,
    str::FromStr,
    sync::Arc,
    task::{Context, Poll},
};

use apalis_core::{
    backend::{
        codec::{json::JsonCodec, Codec, Decoder, Encoder},
        Backend, BackendWithSink, TaskSink,
    },
    error::{BoxDynError, WorkerError},
    task::{attempt::Attempt, status::Status, task_id::TaskId, Metadata, Task},
    utils::Identity,
    worker::{
        context::WorkerContext,
        ext::ack::{Acknowledge, AcknowledgeLayer},
    },
};
use chrono::{DateTime, Utc};
use futures::{
    channel::mpsc::{Receiver, Sender},
    future::BoxFuture,
    lock::Mutex,
    stream::{self, BoxStream},
    FutureExt, Sink, SinkExt, Stream, StreamExt, TryFutureExt,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use sqlx::PgPool;

use crate::{calculate_status, context::SqlContext, postgres::fetcher::PgFetcher, Config};

mod shared;

pub struct PostgresStorage<Args, Fetcher = PgFetcher<Args, JsonCodec<Value>>> {
    _marker: PhantomData<Args>,
    pool: PgPool,
    config: Config,
    fetcher: Option<Fetcher>,
}

async fn register(
    pool: PgPool,
    worker_type: String,
    worker: WorkerContext,
    last_seen: i64,
) -> Result<(), sqlx::Error> {
    let last_seen = DateTime::from_timestamp(last_seen, 0).ok_or(sqlx::Error::Io(
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid Timestamp"),
    ))?;
    let storage_name = "PostgresStorage";
    let res = sqlx::query!(
        "INSERT INTO apalis.workers (id, worker_type, storage_name, layers, last_seen)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (id) DO
                    UPDATE SET 
                        worker_type = EXCLUDED.worker_type,
                        storage_name = EXCLUDED.storage_name,
                        layers = EXCLUDED.layers, 
                        last_seen = NOW()
                WHERE
                    pg_try_advisory_lock(hashtext(workers.id));
                ",
        worker.name(),
        worker_type,
        storage_name,
        worker.get_service(),
        last_seen
    )
    .execute(&pool)
    .await?;
    if res.rows_affected() == 0 {
        return Err(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
            "WORKER_ALREADY_EXISTS",
        )));
    }
    Ok(())
}

impl<Args, D> Backend<Args, SqlContext> for PostgresStorage<Args, PgFetcher<Args, D>>
where
    Args: Send + 'static + Unpin,
    D: Decoder<Args, Compact = Value> + 'static,
    D::Error: std::error::Error + Send + Sync + 'static,
{
    type Error = sqlx::Error;

    type Stream = PgFetcher<Args, D>;

    type Beat = BoxStream<'static, Result<(), sqlx::Error>>;

    type Layer = AcknowledgeLayer<PgAck>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        let worker_type = "apalis::sql".to_owned();
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
        Self::Stream::new(self.pool, self.config, worker.clone())
    }
}

pub struct PgSink<Args, Codec = JsonCodec<Value>> {
    _marker: PhantomData<(Args, Codec)>,
    pool: PgPool,
    buffer: Vec<Task<Value, SqlContext>>,
    config: Config,
    flush_future: Option<Pin<Box<dyn Future<Output = Result<(), sqlx::Error>> + Send>>>,
}

impl<Args> PgSink<Args> {
    pub fn new(pool: PgPool, config: Config) -> Self {
        Self {
            _marker: PhantomData,
            pool,
            buffer: Vec::new(),
            config,
            flush_future: None,
        }
    }
}

impl<Args, E> Sink<Task<Args, SqlContext>> for PgSink<Args, E>
where
    Args: Unpin + Send + Sync + 'static,
    E: Encoder<Args, Compact = Value> + Unpin,
    E::Error: std::error::Error + Send + Sync + 'static,
{
    type Error = sqlx::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Always ready to accept more items into the buffer
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Task<Args, SqlContext>) -> Result<(), Self::Error> {
        // Add the item to the buffer
        self.get_mut()
            .buffer
            .push(item.try_map(|s| E::encode(&s).map_err(|e| sqlx::Error::Encode(e.into())))?);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        // If there's no existing future and buffer is empty, we're done
        if this.flush_future.is_none() && this.buffer.is_empty() {
            return Poll::Ready(Ok(()));
        }

        // Create the future only if we don't have one and there's work to do
        if this.flush_future.is_none() && !this.buffer.is_empty() {
            let pool = this.pool.clone();
            let buffer = std::mem::take(&mut this.buffer);
            let job_type = this.config.namespace().to_owned();

            let fut = async move {
                if buffer.is_empty() {
                    return Ok(());
                }

                // Build the multi-row INSERT with UNNEST
                let mut ids = Vec::new();
                let mut job_data = Vec::new();
                let mut run_ats = Vec::new();
                let mut priorities = Vec::new();
                let mut max_attempts_vec = Vec::new();

                for request in buffer {
                    ids.push(request.meta.task_id.to_string());
                    job_data.push(request.args);
                    run_ats.push(request.meta.context.run_at().clone());
                    priorities.push(*request.meta.context.priority());
                    max_attempts_vec.push(request.meta.context.max_attempts());
                }

                sqlx::query!(
                    r#"
                    INSERT INTO apalis.jobs (id, job_type, job, status, attempts, max_attempts, run_at, priority)
                    SELECT 
                        unnest($1::text[]) as id,
                        $2::text as job_type,
                        unnest($3::jsonb[]) as job,
                        'Pending' as status,
                        0 as attempts,
                        unnest($4::integer[]) as max_attempts,
                        unnest($5::timestamptz[]) as run_at,
                        unnest($6::integer[]) as priority
                    "#,
                    &ids,
                    &job_type,
                    &job_data,
                    &max_attempts_vec,
                    &run_ats,
                    &priorities
                )
                .execute(&pool)
                .await?;

                Ok::<(), sqlx::Error>(())
            };

            this.flush_future = Some(Box::pin(fut));
        }

        // Poll the existing future
        if let Some(mut fut) = this.flush_future.take() {
            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    // Future completed successfully, don't put it back
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => {
                    // Future completed with error, don't put it back
                    Poll::Ready(Err(e))
                }
                Poll::Pending => {
                    // Future is still pending, put it back and return Pending
                    this.flush_future = Some(fut);
                    Poll::Pending
                }
            }
        } else {
            // No future and no work to do
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Flush any remaining items before closing
        self.poll_flush(cx)
    }
}

impl<Args: Send + Sync + Unpin + 'static + Serialize + DeserializeOwned>
    BackendWithSink<Args, SqlContext> for PostgresStorage<Args>
{
    type Sink = PgSink<Args>;
    fn sink(&self) -> Self::Sink {
        PgSink {
            _marker: PhantomData,
            buffer: Vec::new(),
            config: self.config.clone(),
            pool: self.pool.clone(),
            flush_future: None,
        }
    }
}

#[derive(Clone)]
pub struct PgAck {
    pool: PgPool,
}

impl<Res: Serialize> Acknowledge<Res, SqlContext> for PgAck {
    type Error = sqlx::Error;
    type Future = BoxFuture<'static, Result<(), Self::Error>>;
    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        parts: &Metadata<SqlContext>,
    ) -> Self::Future {
        let task_id = parts.task_id.clone();
        let worker_id = parts
            .context
            .lock_by()
            .clone()
            .unwrap_or("rango-tango".to_owned());
        let response = serde_json::to_string(&res.as_ref().map_err(|e| e.to_string()))
            .expect("Could not convert response to JSON");
        let status = calculate_status(&parts.context, res);
        let attempt = parts.attempt.current() as i32;
        let now = Utc::now();
        let pool = self.pool.clone();
        async move {
            let res = sqlx::query!(
                r#"
                            UPDATE apalis.jobs 
                            SET 
                                status = $5,
                                attempts = $2,
                                last_error = $3,
                                done_at = $4
                            WHERE id = $1 AND lock_by = $6
                            "#,
                &task_id.to_string(),
                attempt,
                &response,
                now,
                status.to_string(),
                worker_id
            )
            .execute(&pool)
            .await?;

            if res.rows_affected() == 0 {
                return Err(sqlx::Error::RowNotFound);
            }
            Ok(())
        }
        .boxed()
    }
}

pub struct PgTask {
    job: Value,
    id: Option<String>,
    job_type: Option<String>,
    status: Option<String>,
    attempts: Option<i32>,
    max_attempts: Option<i32>,
    run_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
    lock_at: Option<DateTime<Utc>>,
    lock_by: Option<String>,
    done_at: Option<DateTime<Utc>>,
    priority: Option<i32>,
}

// #[derive(Debug, Serialize, Deserialize)]
// pub enum WorkerConfig {
//     Timeout { duration: Duration },
//     LoadShed,
//     RateLimit { num: u64, per: Duration },
//     ConcurrencyLimit { max: usize },
//     Buffer { bound: usize },
// }

impl PgTask {
    fn try_into_req<D: Decoder<Args, Compact = Value>, Args>(
        self,
    ) -> Result<Task<Args, SqlContext>, sqlx::Error>
    where
        D::Error: std::error::Error + Send + Sync + 'static,
    {
        let args = D::decode(&self.job).map_err(|e| sqlx::Error::Decode(e.into()))?;
        let parts = Metadata {
            attempt: Attempt::new_with_value(
                self.attempts
                    .ok_or(sqlx::Error::ColumnNotFound("attempts".to_owned()))?
                    as usize,
            ),
            status: self
                .status
                .ok_or(sqlx::Error::ColumnNotFound("status".to_owned()))
                .and_then(|s| Status::from_str(&s).map_err(|e| sqlx::Error::Decode(e.into())))?,
            task_id: self
                .id
                .ok_or(sqlx::Error::ColumnNotFound("task_id".to_owned()))
                .and_then(|s| TaskId::from_str(&s).map_err(|e| sqlx::Error::Decode(e.into())))?,
            context: {
                let mut ctx = SqlContext::default();
                ctx.set_run_at(
                    self.run_at
                        .ok_or(sqlx::Error::ColumnNotFound("run_at".to_owned()))?,
                );
                ctx.set_lock_at(self.lock_at.map(|s| s.timestamp()));
                ctx.set_lock_by(self.lock_by);
                // TODO: complete the rest
                ctx
            },

            ..Default::default()
        };
        Ok(Task::new_with_parts(args, parts))
    }
}

mod fetcher {
    use std::{
        collections::VecDeque,
        marker::PhantomData,
        pin::Pin,
        task::{Context, Poll},
        time::{Duration, Instant},
    };

    use apalis_core::{
        backend::codec::{json::JsonCodec, Decoder},
        task::Task,
        timer::Delay,
        worker::context::WorkerContext,
    };
    use futures::{
        future::BoxFuture,
        stream::{self, Stream},
        Future, FutureExt, StreamExt,
    };
    use pin_project::pin_project;
    use serde::de::DeserializeOwned;
    use serde_json::Value;
    use sqlx::{PgPool, Pool, Postgres};

    use crate::{context::SqlContext, postgres::PgTask, Config};

    async fn fetch_next<T, D: Decoder<T, Compact = Value>>(
        pool: PgPool,
        config: Config,
        worker: WorkerContext,
    ) -> Result<Vec<Task<T, SqlContext>>, sqlx::Error>
    where
        D::Error: std::error::Error + Send + Sync + 'static,
    {
        use futures::TryFutureExt;
        let job_type = &config.namespace;
        let buffer_size = config.buffer_size as i32;

        sqlx::query_as!(
            PgTask,
            "Select * from apalis.get_jobs($1, $2, $3)",
            worker.name(),
            job_type,
            buffer_size
        )
        .fetch_all(&pool)
        .await?
        .into_iter()
        .map(|r| r.try_into_req::<D, T>())
        .collect()
    }

    enum StreamState<T> {
        Ready,
        Delay(Delay),
        Fetch(BoxFuture<'static, Result<Vec<Task<T, SqlContext>>, sqlx::Error>>),
        Buffered(VecDeque<Task<T, SqlContext>>),
        Empty,
    }

    #[pin_project(PinnedDrop)]
    pub struct PgFetcher<Args, Decode = JsonCodec<Value>> {
        pool: Pool<Postgres>,
        config: Config,
        wrk: WorkerContext,
        #[pin]
        state: StreamState<Args>,
        current_backoff: Duration,
        last_fetch_time: Option<Instant>,
        decode: PhantomData<Decode>,
    }

    impl<Args, D> PgFetcher<Args, D> {
        pub fn new(pool: Pool<Postgres>, config: Config, wrk: WorkerContext) -> Self {
            let initial_backoff = config.poll_interval;
            Self {
                pool,
                config: config.clone(),
                wrk,
                state: StreamState::Delay(Delay::new(initial_backoff)),
                current_backoff: initial_backoff,
                last_fetch_time: None,
                decode: PhantomData,
            }
        }

        fn next_backoff(&self, current: Duration) -> Duration {
            let doubled = current * 2;
            std::cmp::min(doubled, Duration::from_secs(60 * 5))
        }
    }

    impl<Args: Send + 'static + Unpin, D: Decoder<Args, Compact = Value> + 'static> Stream
        for PgFetcher<Args, D>
    where
        D::Error: std::error::Error + Send + Sync + 'static,
    {
        type Item = Result<Option<Task<Args, SqlContext>>, sqlx::Error>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let mut this = self.get_mut();

            loop {
                match this.state {
                    StreamState::Ready => {
                        let stream = fetch_next::<Args, D>(
                            this.pool.clone(),
                            this.config.clone(),
                            this.wrk.clone(),
                        );
                        this.state = StreamState::Fetch(stream.boxed());
                    }
                    StreamState::Delay(ref mut delay) => match Pin::new(delay).poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(_) => this.state = StreamState::Ready,
                    },

                    StreamState::Fetch(ref mut fut) => match fut.poll_unpin(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(item) => match item {
                            Ok(requests) => {
                                if requests.is_empty() {
                                    let next = this.next_backoff(this.current_backoff);
                                    this.current_backoff = next;
                                    let delay = Delay::new(this.current_backoff);
                                    this.state = StreamState::Delay(delay);
                                } else {
                                    let mut buffer = VecDeque::new();
                                    for request in requests {
                                        buffer.push_back(request);
                                    }
                                    this.current_backoff = this.config.poll_interval;
                                    this.state = StreamState::Buffered(buffer);
                                }
                            }
                            Err(_) => {
                                let next = this.next_backoff(this.current_backoff);
                                this.current_backoff = next;
                                this.state = StreamState::Delay(Delay::new(next));
                                return Poll::Ready(Some(Ok(None)));
                            }
                        },
                    },

                    StreamState::Buffered(ref mut buffer) => {
                        if let Some(request) = buffer.pop_front() {
                            // Yield the next buffered item
                            if buffer.is_empty() {
                                // Buffer is now empty, transition to ready for next fetch
                                this.state = StreamState::Ready;
                            }
                            return Poll::Ready(Some(Ok(Some(request))));
                        } else {
                            // Buffer is empty, transition to ready
                            this.state = StreamState::Ready;
                        }
                    }

                    StreamState::Empty => return Poll::Ready(None),
                }
            }
        }
    }

    #[pin_project::pinned_drop]
    impl<Args, D> PinnedDrop for PgFetcher<Args, D> {
        fn drop(self: Pin<&mut Self>) {
            match &self.state {
                StreamState::Buffered(remaining) => {
                    println!("dropped with items in buffer {}", remaining.len());
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, time::Duration};

    use chrono::Local;

    use apalis_core::{
        backend::memory::MemoryStorage,
        error::BoxDynError,
        worker::{builder::WorkerBuilder, event::Event, ext::event_listener::EventListenerExt},
    };
    use tower::limit::ConcurrencyLimitLayer;

    use super::*;

    #[tokio::test]
    async fn basic_worker() {
        let backend = PostgresStorage {
            _marker: PhantomData,
            pool: PgPool::connect("postgres://postgres:postgres@localhost/apalis_dev")
                .await
                .unwrap(),
            config: Config::default(),
            fetcher: None,
        };

        let mut sink = backend.sink();

        let task = Task::builder(Default::default())
            .run_after(Duration::from_secs(5))
            .build();

        sink.send(task).await.unwrap();

        async fn send_reminder(
            _: HashMap<String, String>,
            ctx: SqlContext,
        ) -> Result<(), BoxDynError> {
            // tokio::time::sleep(Duration::from_secs(2)).await;
            Err("Failed".into())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
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
        worker.run().await.unwrap();
    }
}
