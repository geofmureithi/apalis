use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

pub enum SqlSinkError<EncodeError, DbErr> {
    EncodeError(EncodeError),
    DatabaseError(DbErr),
}

use apalis_core::{backend::codec::Codec, task::Task};
use futures::{future::BoxFuture, Sink};

pub struct SqlSink<DB, Config, Fn, Args, Meta, IdType, Compact, Encode, Error> {
    _marker: PhantomData<(Args, Encode)>,
    pool: DB,
    pub buffer: Vec<Task<Compact, Meta, IdType>>,
    config: Config,
    flush_future: Option<Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>>,
    sink_fn: Arc<Fn>,
}

impl<DB, Config, Fn, Args, Meta, IdType, Compact, Encode, Error> Clone
    for SqlSink<DB, Config, Fn, Args, Meta, IdType, Compact, Encode, Error>
where
    DB: Clone,
    Config: Clone,
{
    fn clone(&self) -> Self {
        Self {
            _marker: PhantomData,
            pool: self.pool.clone(),
            buffer: Vec::new(),
            config: self.config.clone(),
            flush_future: None,
            sink_fn: self.sink_fn.clone(),
        }
    }
}

impl<DB, Config, Fn, Args, Meta, IdType, Compact, Encode, Error>
    SqlSink<DB, Config, Fn, Args, Meta, IdType, Compact, Encode, Error>
{
    pub fn new(pool: DB, config: Config, sink_fn: Fn) -> Self {
        Self {
            _marker: PhantomData,
            pool,
            buffer: Vec::new(),
            config,
            flush_future: None,
            sink_fn: Arc::new(sink_fn),
        }
    }
}

impl<DB, Config, FnT, Args, Meta, IdType, Compact, Encode, Error> Sink<Task<Args, Meta, IdType>>
    for SqlSink<DB, Config, FnT, Args, Meta, IdType, Compact, Encode, Error>
where
    Args: Unpin + Send + Sync + 'static,
    Encode: Codec<Args, Compact = Compact> + Unpin,
    Encode::Error: std::error::Error + Send + Sync + 'static,
    Compact: Unpin + Send + 'static,
    FnT: Fn(DB, Config, Vec<Task<Compact, Meta, IdType>>) -> BoxFuture<'static, Result<(), Error>>
        + Unpin
        + Send
        + 'static,
    Config: Clone + Unpin + Send + 'static,
    DB: Clone + Unpin + Send + 'static,
    Meta: Unpin + Send + 'static,
    IdType: Sync + Send + Unpin + 'static,
    Error: 'static,
{
    type Error = SqlSinkError<Encode::Error, Error>;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Always ready to accept more items into the buffer
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Task<Args, Meta, IdType>) -> Result<(), Self::Error> {
        // Add the item to the buffer
        self.get_mut()
            .buffer
            .push(item.try_map(|s| Encode::encode(&s).map_err(|e| SqlSinkError::EncodeError(e)))?);
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
            let config = this.config.clone();
            let buffer = std::mem::take(&mut this.buffer);
            let sink_fut = (this.sink_fn)(pool, config, buffer);
            this.flush_future = Some(Box::pin(sink_fut));
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
                    Poll::Ready(Err(SqlSinkError::DatabaseError(e)))
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
        self.poll_flush(cx)
    }
}
