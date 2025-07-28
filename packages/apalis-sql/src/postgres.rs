use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use apalis_core::{
    backend::{codec::Codec, Backend}, error::WorkerError, request::{task_id::TaskId, Request}, worker::context::WorkerContext
};
use chrono::{DateTime, Utc};
use futures::{
    channel::mpsc::{Receiver, Sender},
    lock::Mutex, stream::BoxStream,
};
use serde::de::DeserializeOwned;
use serde_json::Value;
use sqlx::PgPool;

use crate::{context::SqlContext, postgres::fetcher::PgFetcher, Config};

pub trait Channel<Args> {
    type Sink;
    type Stream;
}

pub struct PostgresStorage<Args, Poller = PgFetcher<Args>, Sink = PgSink<Args>> {
    _marker: PhantomData<Args>,
    pool: PgPool,
    sink: Sink,
    poller: Poller,
}

impl <Args, Fetch, Sink> Backend<Args, SqlContext> for PostgresStorage<Args, Fetch, Sink> {
    type Error= sqlx::Error;

    type Stream = Fetch;

    type Beat = BoxStream<'static, Result<(), WorkerError>>;

    type Layer = ();

    type Sink = Sink;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        todo!()
    }

    fn middleware(&self) -> Self::Layer {
        todo!()
    }

    fn sink(&self) -> Self::Sink {
        todo!()
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        todo!()
    }
}

pub struct PgSink<Args> {
    _marker: PhantomData<Args>,
    pool: PgPool,
}

pub struct SharedPostgresStorage {
    pool: PgPool,
    registry: Arc<Mutex<HashMap<String, Arc<Sender<TaskId>>>>>,
}

struct PgTask {
    args: Value,
    id: String,
    attempt: u32,
    status: String,
    run_at: DateTime<Utc>,
    max_attempts: u32,
    last_error: Option<String>,
    lock_at: Option<i64>,
    lock_by: Option<String>,
    done_at: Option<i64>,
    priority: i32,
}

mod fetcher {
    use std::{
        marker::PhantomData,
        pin::Pin,
        task::{Context, Poll},
        time::Duration,
    };

    use apalis_core::{request::Request, timer::Delay, worker::context::WorkerContext};
    use futures::{stream::Stream, Future};
    use pin_project_lite::pin_project;
    use sqlx::{PgPool, Pool, Postgres};

    use crate::{context::SqlContext, postgres::PgTask, Config};

    fn fetch_next<T>(
        pool: &mut PgPool,
        config: &Config,
        worker: &WorkerContext,
    ) -> impl Stream<Item = Result<Vec<Request<T, SqlContext>>, sqlx::Error>> {
        let job_type = &config.namespace;

        let jobs = sqlx::query_as!(PgTask, "Select * from apalis.get_jobs($1, $2, $3)")
            // .bind(worker.name().to_string())
            // .bind(job_type)
            // // https://docs.rs/sqlx/latest/sqlx/postgres/types/index.html
            // .bind(
            //     i32::try_from(config.buffer_size)
            //         .map_err(|e| sqlx::Error::Io(io::Error::new(io::ErrorKind::InvalidInput, e)))?,
            // )
            .fetch_many(&mut pool);
        jobs
    }

    
type BoxedRequestStream<T> =
    Pin<Box<dyn Stream<Item = Result<Request<T, SqlContext>, sqlx::Error>> + Send>>;

enum StreamState<T> {
    Delay(Delay),
    Fetch(BoxedRequestStream<T>),
    Empty,
}

// ----------- PgFetcher Stream ------------

pin_project! {
    pub struct PgFetcher<Args> {
        pool: Pool<Postgres>,
        config: Config,
        wrk: WorkerContext,
        #[pin]
        state: StreamState<Args>,
        backoff: Duration,
    }
}

impl<T: Send + 'static, Args: 'static> PgFetcher<T, Args> {
    pub fn new(pool: Pool<Postgres>, config: Config, wrk: WorkerContext) -> Self {
        let initial_backoff = config.poll_interval;
        Self {
            pool,
            config: config.clone(),
            wrk,
            state: StreamState::Delay(Delay::new(initial_backoff)),
            backoff: initial_backoff,
            _phantom: PhantomData,
        }
    }

    fn next_backoff(&self, current: Duration) -> Duration {
        let doubled = current * 2;
        if doubled > self.config.max_backoff {
            self.config.max_backoff
        } else {
            doubled
        }
    }
}

impl<T: Send + 'static, Args: 'static> Stream for PgFetcher<T, Args> {
    type Item = Result<Request<T, SqlContext>, sqlx::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            match this.state {
                StreamState::Delay(ref mut delay) => {
                    match Pin::new(delay).poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(_) => {
                            let stream = fetch_next::<T>(
                                this.pool.clone(),
                                this.config.clone(),
                                this.wrk.clone(),
                            );
                            *this.state = StreamState::Fetch(stream);
                        }
                    }
                }

                StreamState::Fetch(ref mut stream) => {
                    match stream.as_mut().poll_next(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Some(item)) => {
                            match &item {
                                Ok(_) => {
                                    *this.backoff = this.config.poll_interval;
                                }
                                Err(_) => {
                                    let next = this.next_backoff(*this.backoff);
                                    *this.backoff = next;
                                    *this.state = StreamState::Delay(Delay::new(next));
                                }
                            }
                            return Poll::Ready(Some(item));
                        }
                        Poll::Ready(None) => {
                            *this.state = StreamState::Delay(Delay::new(*this.backoff));
                            return Poll::Pending;
                        }
                    }
                }

                StreamState::Empty => return Poll::Ready(None),
            }
        }
    }
}
}
