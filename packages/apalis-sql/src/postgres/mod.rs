use std::{
    backtrace::Backtrace,
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
    task::{
        attempt::Attempt,
        status::Status,
        task_id::{TaskId, Ulid},
        Metadata, Task,
    },
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
use serde_json::{json, Value};
use sqlx::PgPool;

use crate::{calculate_status, context::SqlContext, postgres::fetcher::PgFetcher, Config};

mod custom;
mod shared;

#[cfg(feature = "postgres")]
pub type CompactT = Value;

#[cfg(feature = "postgres-bytes")]
pub type CompactT = Vec<u8>;

pub struct PostgresStorage<
    Args,
    Compact = CompactT,
    Codec = JsonCodec<CompactT>,
    Fetcher = DefaultFetcher,
> {
    _marker: PhantomData<(Args, Compact, Codec)>,
    pool: PgPool,
    config: Config,
    fetcher: Fetcher,
}

pub struct DefaultFetcher(());

impl<Args> PostgresStorage<Args> {
    /// Creates a new PostgresStorage instance.
    pub fn new(pool: PgPool, config: Config) -> Self {
        Self {
            _marker: PhantomData,
            pool,
            config,
            fetcher: DefaultFetcher(()),
        }
    }

    /// Returns a reference to the pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Returns a mutable reference to the pool.
    pub fn pool_mut(&mut self) -> &mut PgPool {
        &mut self.pool
    }

    /// Returns a reference to the config.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns a mutable reference to the config.
    pub fn config_mut(&mut self) -> &mut Config {
        &mut self.config
    }

    // /// Returns a reference to the fetcher (if any).
    // pub fn fetcher(&self) -> Option<&Fetcher> {
    //     self.fetcher.as_ref()
    // }

    // /// Returns a mutable reference to the fetcher (if any).
    // pub fn fetcher_mut(&mut self) -> Option<&mut Fetcher> {
    //     self.fetcher.as_mut()
    // }

    // /// Sets the fetcher.
    // pub fn set_fetcher(&mut self, fetcher: Fetcher) {
    //     self.fetcher = Some(fetcher);
    // }

    // /// Clears the fetcher.
    // pub fn clear_fetcher(&mut self) {
    //     self.fetcher = None;
    // }
}

pub(crate) async fn register(
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

impl<Args, Decode> Backend<Args, SqlContext>
    for PostgresStorage<Args, CompactT, Decode, DefaultFetcher>
where
    Args: Send + 'static + Unpin,
    Decode: Decoder<Args, Compact = CompactT> + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
    // Compact: 'static + Send + Unpin,
{
    type IdType = Ulid;

    type Error = sqlx::Error;

    type Stream = PgFetcher<Args, CompactT, Decode>;

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
        Self::Stream::new(&self.pool, &self.config, worker)
    }
}

pub struct PgSink<Args, Compact = CompactT, Codec = JsonCodec<CompactT>> {
    _marker: PhantomData<(Args, Codec)>,
    pool: PgPool,
    buffer: Vec<Task<Compact, SqlContext, Ulid>>,
    config: Config,
    flush_future: Option<Pin<Box<dyn Future<Output = Result<(), sqlx::Error>> + Send>>>,
}

impl<Args, Compact, Codec> Clone for PgSink<Args, Compact, Codec>
where
    PgPool: Clone,
    Config: Clone,
{
    fn clone(&self) -> Self {
        Self {
            _marker: PhantomData,
            pool: self.pool.clone(),
            buffer: Vec::new(),
            config: self.config.clone(),
            flush_future: None,
        }
    }
}

impl<Args, Encode> PgSink<Args, CompactT, Encode> {
    pub fn new(pool: &PgPool, config: &Config) -> Self {
        Self {
            _marker: PhantomData,
            pool: pool.clone(),
            buffer: Vec::new(),
            config: config.clone(),
            flush_future: None,
        }
    }
}

impl<Args, Encode> Sink<Task<Args, SqlContext, Ulid>> for PgSink<Args, CompactT, Encode>
where
    Args: Unpin + Send + Sync + 'static,
    Encode: Encoder<Args, Compact = CompactT> + Unpin,
    Encode::Error: std::error::Error + Send + Sync + 'static,
    // Compact: Unpin + Send + 'static, // for<'a> &'a [Compact]: sqlx::Encode<'a, sqlx::Postgres>
{
    type Error = sqlx::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Always ready to accept more items into the buffer
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Task<Args, SqlContext, Ulid>) -> Result<(), Self::Error> {
        // Add the item to the buffer
        self.get_mut()
            .buffer
            .push(item.try_map(|s| Encode::encode(&s).map_err(|e| sqlx::Error::Encode(e.into())))?);
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

                for task in buffer {
                    ids.push(
                        task.meta
                            .task_id
                            .map(|id| id.to_string())
                            .unwrap_or(Ulid::new().to_string()),
                    );
                    job_data.push(task.args);
                    run_ats.push(
                        DateTime::from_timestamp(task.meta.run_at as i64, 0)
                            .ok_or(sqlx::Error::ColumnNotFound("run_at".to_owned()))?,
                    );
                    priorities.push(*task.meta.context.priority());
                    max_attempts_vec.push(task.meta.context.max_attempts());
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

impl<Args, Fetch, Encode> BackendWithSink<Args, SqlContext>
    for PostgresStorage<Args, CompactT, Encode, Fetch>
where
    PostgresStorage<Args, CompactT, Encode, Fetch>: Backend<Args, SqlContext, IdType = Ulid>,
    Args: Send + Sync + Unpin + 'static + Serialize + DeserializeOwned,
    Encode: Encoder<Args, Compact = CompactT> + Unpin,
    Encode::Error: std::error::Error + Send + Sync + 'static,
    // Compact: Unpin + Send + 'static,
{
    type Sink = PgSink<Args, CompactT, Encode>;
    fn sink(&mut self) -> Self::Sink {
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
impl PgAck {
    fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl<Res: Serialize> Acknowledge<Res, SqlContext, Ulid> for PgAck {
    type Error = sqlx::Error;
    type Future = BoxFuture<'static, Result<(), Self::Error>>;
    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        parts: &Metadata<SqlContext, Ulid>,
    ) -> Self::Future {
        let task_id = parts.task_id.clone();
        let worker_id = parts.context.lock_by().clone();

        let response = serde_json::to_string(&res.as_ref().map_err(|e| e.to_string()));
        let status = calculate_status(&parts.context, res);
        let attempt = parts.attempt.current() as i32;
        let now = Utc::now();
        let pool = self.pool.clone();
        async move {
            let res = sqlx::query!(
                r#"
                            UPDATE apalis.jobs 
                            SET 
                                status = $4,
                                attempts = $2,
                                last_error = $3,
                                done_at = NOW()
                            WHERE id = $1 AND lock_by = $5
                            "#,
                task_id
                    .ok_or(sqlx::Error::ColumnNotFound("TASK_ID_FOR_ACK".to_owned()))?
                    .to_string(),
                attempt,
                &response.map_err(|e| sqlx::Error::Decode(e.into()))?,
                status.to_string(),
                worker_id.ok_or(sqlx::Error::ColumnNotFound("WORKER_ID_LOCK_BY".to_owned()))?
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

#[derive(Debug)]
pub struct PgTask<Compact = CompactT> {
    job: Compact,
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
    fn try_into_req<D: Decoder<Args, Compact = CompactT>, Args>(
        self,
    ) -> Result<Task<Args, SqlContext, Ulid>, sqlx::Error>
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
            task_id: Some(
                self.id
                    .ok_or(sqlx::Error::ColumnNotFound("task_id".to_owned()))
                    .and_then(|s| {
                        TaskId::from_str(&s).map_err(|e| sqlx::Error::Decode(e.into()))
                    })?,
            ),
            run_at: self
                .run_at
                .ok_or(sqlx::Error::ColumnNotFound("run_at".to_owned()))?
                .timestamp() as u64,
            context: {
                let mut ctx = SqlContext::default();
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
        sync::Arc,
        task::{Context, Poll},
        time::{Duration, Instant},
    };

    use apalis_core::{
        backend::codec::{json::JsonCodec, Decoder},
        task::{task_id::Ulid, Task},
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

    use crate::{
        context::SqlContext,
        postgres::{CompactT, PgTask},
        Config,
    };

    async fn fetch_next<Args, D: Decoder<Args, Compact = CompactT>>(
        pool: PgPool,
        config: Config,
        worker: WorkerContext,
    ) -> Result<Vec<Task<Args, SqlContext, Ulid>>, sqlx::Error>
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
        .map(|r| r.try_into_req::<D, Args>())
        .collect()
    }

    enum StreamState<Args> {
        Ready,
        Delay(Delay),
        Fetch(BoxFuture<'static, Result<Vec<Task<Args, SqlContext, Ulid>>, sqlx::Error>>),
        Buffered(VecDeque<Task<Args, SqlContext, Ulid>>),
        Empty,
    }

    #[pin_project(PinnedDrop)]
    pub struct PgFetcher<Args, Compact = Value, Decode = JsonCodec<Value>> {
        pool: Pool<Postgres>,
        config: Config,
        wrk: WorkerContext,
        #[pin]
        state: StreamState<Args>,
        current_backoff: Duration,
        last_fetch_time: Option<Instant>,
        fetch_fn: Arc<
            Box<
                dyn Fn(
                        Pool<Postgres>,
                        Config,
                        WorkerContext,
                    )
                        -> BoxFuture<'static, Result<Vec<Task<Args, SqlContext, Ulid>>, sqlx::Error>>
                    + Send
                    + Sync,
            >,
        >,
        _marker: PhantomData<(Compact, Decode)>,
    }

    impl<Args, Compact, Decode> Clone for PgFetcher<Args, Compact, Decode> {
        fn clone(&self) -> Self {
            Self {
                pool: self.pool.clone(),
                config: self.config.clone(),
                wrk: self.wrk.clone(),
                state: StreamState::Ready,
                current_backoff: self.current_backoff,
                last_fetch_time: self.last_fetch_time,
                fetch_fn: self.fetch_fn.clone(),
                _marker: PhantomData,
            }
        }
    }

    impl<Args: 'static, Decode> PgFetcher<Args, CompactT, Decode>
    where
        Decode: Decoder<Args, Compact = CompactT> + 'static,
        Decode::Error: std::error::Error + Send + Sync + 'static,
    {
        pub fn new(
            pool: &Pool<Postgres>,
            config: &Config,
            wrk: &WorkerContext,
            // fetch_fn: Box<
            //     dyn Fn(
            //             Pool<Postgres>,
            //             Config,
            //             WorkerContext,
            //         )
            //             -> BoxFuture<'static, Result<Vec<Task<Args, SqlContext>>, sqlx::Error>>
            //         + Send
            //         + Sync,
            // >,
        ) -> Self {
            let initial_backoff = config.poll_interval;
            Self {
                pool: pool.clone(),
                config: config.clone(),
                wrk: wrk.clone(),
                state: StreamState::Delay(Delay::new(initial_backoff)),
                current_backoff: initial_backoff,
                last_fetch_time: None,
                fetch_fn: Arc::new(Box::new(|pool, config, worker| {
                    fetch_next::<Args, Decode>(pool, config, worker).boxed()
                })),
                _marker: PhantomData,
            }
        }

        fn next_backoff(&self, current: Duration) -> Duration {
            let doubled = current * 2;
            std::cmp::min(doubled, Duration::from_secs(60 * 5))
        }
    }

    impl<Args, Decode> Stream for PgFetcher<Args, CompactT, Decode>
    where
        Decode::Error: std::error::Error + Send + Sync + 'static,
        Args: Send + 'static + Unpin,
        Decode: Decoder<Args, Compact = CompactT> + 'static,
        // Compact: Unpin + Send + 'static,
    {
        type Item = Result<Option<Task<Args, SqlContext, Ulid>>, sqlx::Error>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let mut this = self.get_mut();

            loop {
                match this.state {
                    StreamState::Ready => {
                        let stream = (this.fetch_fn)(
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
    impl<Args, Compact, Decode> PinnedDrop for PgFetcher<Args, Compact, Decode> {
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
        let mut backend = PostgresStorage::new(
            PgPool::connect("postgres://postgres:postgres@localhost/apalis_dev")
                .await
                .unwrap(),
            Default::default(),
        );

        let mut sink = backend.sink();

        let task = Task::builder(Default::default())
            .run_after(Duration::from_secs(5))
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
            .run_after(Duration::from_secs(5))
            .build();

        sink.send_all(&mut stream::iter(vec![task, task2].into_iter().map(Ok)))
            .await
            .unwrap();

        async fn send_reminder(
            _: HashMap<String, String>,
            ctx: SqlContext,
        ) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(2)).await;
            Ok(())
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
