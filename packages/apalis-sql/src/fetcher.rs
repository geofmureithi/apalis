use std::{
    collections::VecDeque,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::Poll,
    time::{Duration, Instant},
};

use apalis_core::{
    backend::codec::Decoder, task::Task, timer::Delay, worker::context::WorkerContext,
};
use futures::{future::BoxFuture, FutureExt, Stream};
use pin_project::pin_project;

pub enum StreamState<Args, Meta, IdType, Error> {
    Ready,
    Delay(Delay),
    Fetch(BoxFuture<'static, Result<Vec<Task<Args, Meta, IdType>>, Error>>),
    Buffered(VecDeque<Task<Args, Meta, IdType>>),
    Empty,
}

/// An agnostic fetcher that can be used to build backends
///
#[pin_project(PinnedDrop)]
pub struct SqlFetcher<DB, Config, Fn, Args, Meta, IdType, Compact, Decode, Error> {
    pool: DB,
    config: Config,
    wrk: WorkerContext,
    #[pin]
    pub state: StreamState<Args, Meta, IdType, Error>,
    current_backoff: Duration,
    last_fetch_time: Option<Instant>,
    fetch_fn: Arc<Fn>,
    _marker: PhantomData<(Compact, Decode)>,
}

impl<DB, Config, Fn, Args, Meta, IdType, Compact, Decode, Error> Clone
    for SqlFetcher<DB, Config, Fn, Args, Meta, IdType, Compact, Decode, Error>
where
    DB: Clone,
    Config: Clone,
{
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

impl<DB, Config, Fn, Args, Meta, IdType, Compact, Decode, Error>
    SqlFetcher<DB, Config, Fn, Args, Meta, IdType, Compact, Decode, Error>
{
    pub fn new(pool: &DB, config: &Config, wrk: &WorkerContext, fetch_fn: Fn) -> Self
    where
        Decode: Decoder<Args, Compact = Compact> + 'static,
        Decode::Error: std::error::Error + Send + Sync + 'static,
        DB: Clone,
        Config: Clone,
    {
        let initial_backoff = Duration::from_secs(1);
        Self {
            pool: pool.clone(),
            config: config.clone(),
            wrk: wrk.clone(),
            state: StreamState::Delay(Delay::new(initial_backoff)),
            current_backoff: initial_backoff,
            last_fetch_time: None,
            fetch_fn: Arc::new(fetch_fn),
            _marker: PhantomData,
        }
    }

    fn next_backoff(&self, current: Duration) -> Duration {
        let doubled = current * 2;
        std::cmp::min(doubled, Duration::from_secs(60 * 5))
    }

    pub fn take_pending(&mut self) -> VecDeque<Task<Args, Meta, IdType>> {
        match &mut self.state {
            StreamState::Buffered(tasks) => std::mem::take(tasks),
            _ => VecDeque::new(),
        }
    }
}

impl<DB, Config, TFn, Args, Meta, IdType, Compact, Decode, Error> Stream
    for SqlFetcher<DB, Config, TFn, Args, Meta, IdType, Compact, Decode, Error>
where
    Decode::Error: std::error::Error + Send + Sync + 'static,
    Args: Send + 'static + Unpin,
    Decode: Decoder<Args, Compact = Compact> + 'static,
    Meta: Unpin + 'static,
    TFn: Fn(
        DB,
        Config,
        WorkerContext,
    ) -> BoxFuture<'static, Result<Vec<Task<Args, Meta, IdType>>, Error>>,
    DB: Clone,
    Config: Clone,
    IdType: 'static,
    Error: 'static,
{
    type Item = Result<Option<Task<Args, Meta, IdType>>, Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.get_mut();

        loop {
            match this.state {
                StreamState::Ready => {
                    let stream =
                        (this.fetch_fn)(this.pool.clone(), this.config.clone(), this.wrk.clone());
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
                                this.current_backoff = Duration::from_secs(1);
                                this.state = StreamState::Buffered(buffer);
                            }
                        }
                        Err(e) => {
                            let next = this.next_backoff(this.current_backoff);
                            this.current_backoff = next;
                            this.state = StreamState::Delay(Delay::new(next));
                            return Poll::Ready(Some(Err(e)));
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
impl<DB, Config, Fn, Args, Meta, IdType, Compact, Decode, Error> PinnedDrop
    for SqlFetcher<DB, Config, Fn, Args, Meta, IdType, Compact, Decode, Error>
{
    fn drop(self: Pin<&mut Self>) {
        match &self.state {
            StreamState::Buffered(remaining) => {
                println!("dropped with items in buffer {}", remaining.len());
            }
            _ => {}
        }
    }
}
