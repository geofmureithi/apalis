//! # In-memory backend based on channels
//!
//! An in-memory backend suitable for testing, prototyping, or lightweight task processing scenarios where persistence is not required.
//!
//! ## Features
//! - Generic in-memory queue for any task type.
//! - Implements [`Backend`] for integration with workers.
//! - Sink support: Ability to push new tasks.
//!
//! A detailed feature list can be found in the [capabilities](crate::backend::memory::MemoryStorage#capabilities) section.
//!
//! ## Example
//!
//! ```rust
//! # use apalis_core::backend::memory::MemoryStorage;
//! # use apalis_core::worker::context::WorkerContext;
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut storage = MemoryStorage::new();
//!     storage.push(42).await.unwrap();
//!
//!     let int_worker = WorkerBuilder::new("rango-tango-int")
//!         .backend(int_store)
//!         .data(Count::default())
//!         .build(task);
//! 
//!     int_worker.run().await.unwrap();
//! }
//! ```
//!
//! ## Note
//! This backend is not persistent and is intended for use cases where durability is not required.
//! For production workloads, consider using a persistent backend such as PostgreSQL or Redis.
//!
//! ## See Also
//! - [`Backend`]
//! - [`WorkerContext`](crate::worker::context::WorkerContext)

use crate::task::extensions::Extensions;

use crate::{
    backend::{Backend, TaskStream},
    task::{
        task_id::{RandomId, TaskId},
        Task,
    },
    worker::context::WorkerContext,
};
use futures_channel::mpsc::{unbounded, SendError};
use futures_core::ready;
use futures_sink::Sink;
use futures_util::{
    stream::{self, BoxStream},
    FutureExt, SinkExt, Stream, StreamExt,
};

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tower_layer::Identity;

/// In-memory queue that is based on channels
///
/// ## Capabilities
/// | Feature | Status | Description            |
/// |---------|--------|------------------------|
/// | Sink support    | ✓      | Ability to push new tasks  |
/// | Codec Support   |  ✗      | Serialization support for arguments. Uses `json`  |
/// | Workflow Support   |  ✗      | Flexible enough to support workflows     |
/// | Ack Support   |  ✗      | Allow acknowledgement of task completion   |
/// | WaitForCompletion   |  ✗     | Wait for tasks to complete without blocking     |
pub struct MemoryStorage<Args, Ctx = Extensions> {
    pub(super) sender: MemorySink<Args, Ctx>,
    pub(super) receiver: Pin<Box<dyn Stream<Item = Task<Args, Ctx>> + Send>>,
}

impl<Args: Send + 'static> MemoryStorage<Args, Extensions> {
    /// Create a new in-memory storage
    pub fn new() -> Self {
        let (sender, receiver) = unbounded();
        let sender = Box::new(sender)
            as Box<dyn Sink<Task<Args, Extensions>, Error = SendError> + Send + Sync + Unpin>;
        MemoryStorage {
            sender: MemorySink {
                inner: Arc::new(futures_util::lock::Mutex::new(sender)),
            },
            receiver: receiver.boxed(),
        }
    }
}

impl<Args, Ctx> Sink<Task<Args, Ctx>> for MemoryStorage<Args, Ctx> {
    type Error = SendError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().sender.poll_ready_unpin(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Task<Args, Ctx>) -> Result<(), Self::Error> {
        self.as_mut().sender.start_send_unpin(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().sender.poll_flush_unpin(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().sender.poll_close_unpin(cx)
    }
}

/// Memory sink for sending tasks to the in-memory backend
pub struct MemorySink<Args, Ctx = Extensions> {
    pub(super) inner: Arc<
        futures_util::lock::Mutex<
            Box<dyn Sink<Task<Args, Ctx>, Error = SendError> + Send + Sync + Unpin + 'static>,
        >,
    >,
}

impl<Args, Ctx> std::fmt::Debug for MemorySink<Args, Ctx> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemorySink")
            .field("inner", &"<Sink>")
            .finish()
    }
}

impl<Args, Ctx> Clone for MemorySink<Args, Ctx> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<Args, Ctx> Sink<Task<Args, Ctx>> for MemorySink<Args, Ctx> {
    type Error = SendError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut lock = ready!(self.inner.lock().poll_unpin(cx));
        Pin::new(&mut *lock).poll_ready_unpin(cx)
    }

    fn start_send(self: Pin<&mut Self>, mut item: Task<Args, Ctx>) -> Result<(), Self::Error> {
        let mut lock = self.inner.try_lock().unwrap();
        // Ensure task has id
        item.ctx
            .task_id
            .get_or_insert_with(|| TaskId::new(RandomId::default()));
        Pin::new(&mut *lock).start_send_unpin(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut lock = ready!(self.inner.lock().poll_unpin(cx));
        Pin::new(&mut *lock).poll_flush_unpin(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut lock = ready!(self.inner.lock().poll_unpin(cx));
        Pin::new(&mut *lock).poll_close_unpin(cx)
    }
}

impl<Args, Ctx> std::fmt::Debug for MemoryStorage<Args, Ctx> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStorage")
            .field("sender", &self.sender)
            .field("receiver", &"<Stream>")
            .finish()
    }
}

impl<Args, Ctx> Stream for MemoryStorage<Args, Ctx> {
    type Item = Task<Args, Ctx>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.as_mut().receiver.poll_next_unpin(cx)
    }
}

// MemoryStorage as a Backend
impl<Args: 'static + Clone + Send, Ctx: 'static + Default> Backend<Args>
    for MemoryStorage<Args, Ctx>
{
    type IdType = RandomId;

    type Ctx = Ctx;

    type Error = SendError;
    type Stream = TaskStream<Task<Args, Ctx>, SendError>;
    type Layer = Identity;
    type Beat = BoxStream<'static, Result<(), Self::Error>>;

    type Codec = ();

    fn heartbeat(&self, _: &WorkerContext) -> Self::Beat {
        stream::once(async { Ok(()) }).boxed()
    }
    fn middleware(&self) -> Self::Layer {
        Identity::new()
    }

    fn poll(self, _worker: &WorkerContext) -> Self::Stream {
        let stream = self.receiver.map(|r| Ok(Some(r))).boxed();
        stream
    }
}
