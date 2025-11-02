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
//! # use apalis_core::worker::builder::WorkerBuilder;
//! # use apalis_core::backend::TaskSink;
//! # async fn task(_: u32, ctx: WorkerContext) { ctx.stop().unwrap();}
//! #[tokio::main]
//! async fn main() {
//!     let mut store = MemoryStorage::new();
//!     store.push(42).await.unwrap();
//!
//!     let worker = WorkerBuilder::new("int-worker")
//!         .backend(store)
//!         .build(task);
//!
//!     worker.run().await.unwrap();
//! }
//! ```
//!
//! ## Note
//! This backend is not persistent and is intended for use cases where durability is not required.
//! For production workloads, consider using a persistent backend such as PostgreSQL or Redis.
//!
//! ## See Also
//! - [`Backend`]
//! - [`WorkerContext`]
use crate::backend::codec::IdentityCodec;
use crate::features_table;
use crate::task::extensions::Extensions;
use crate::{
    backend::{Backend, TaskStream},
    task::{
        Task,
        task_id::{RandomId, TaskId},
    },
    worker::context::WorkerContext,
};
use futures_channel::mpsc::{SendError, unbounded};
use futures_core::ready;
use futures_sink::Sink;
use futures_util::{
    FutureExt, SinkExt, Stream, StreamExt,
    stream::{self, BoxStream},
};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tower_layer::Identity;

/// In-memory queue that is based on channels
///
#[doc = features_table! {
    setup = {
        use apalis_core::backend::memory::MemoryStorage;
        // No migrations
        MemoryStorage::new()
    };,

    Backend => supported("Basic Backend functionality", true),
    TaskSink => supported("Ability to push new tasks", true),
    Serialization => not_supported("Serialization support for arguments"),

    PipeExt => not_implemented("Allow other backends to pipe to this backend"),
    MakeShared => not_supported("Share the same JSON storage across multiple workers"),

    Update => not_supported("Allow updating a task"),
    FetchById => not_supported("Allow fetching a task by its ID"),
    Reschedule => not_supported("Reschedule a task"),

    ResumeById => not_supported("Resume a task by its ID"),
    ResumeAbandoned => not_supported("Resume abandoned tasks"),
    Vacuum => not_supported("Vacuum the task storage"),

    Workflow => not_implemented("Flexible enough to support workflows"),
    WaitForCompletion => not_implemented("Wait for tasks to complete without blocking"), // Requires Clone

    RegisterWorker => not_supported("Allow registering a worker with the backend"),
    ListWorkers => not_supported("List all workers registered with the backend"),
    ListTasks => not_supported("List all tasks in the backend"),
}]
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
        item.parts
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
        self.receiver.poll_next_unpin(cx)
    }
}

// MemoryStorage as a Backend
impl<Args: 'static + Clone + Send, Ctx: 'static + Default> Backend for MemoryStorage<Args, Ctx> {
    type Args = Args;
    type IdType = RandomId;

    type Context = Ctx;

    type Error = SendError;
    type Stream = TaskStream<Task<Args, Ctx>, SendError>;
    type Layer = Identity;
    type Beat = BoxStream<'static, Result<(), Self::Error>>;

    type Codec = IdentityCodec;
    type Compact = Args;

    fn heartbeat(&self, _: &WorkerContext) -> Self::Beat {
        stream::once(async { Ok(()) }).boxed()
    }
    fn middleware(&self) -> Self::Layer {
        Identity::new()
    }

    fn poll(self, _worker: &WorkerContext) -> Self::Stream {
        let stream = self.receiver.boxed().map(|r| Ok(Some(r))).boxed();
        stream
    }
}
