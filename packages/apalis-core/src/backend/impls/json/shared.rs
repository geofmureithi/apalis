/// Sharable JSON based backend.
///
/// The [`SharedJsonStore`] allows multiple task types to be stored
/// and processed concurrently using a single JSON-based in-memory backend.
/// It is useful for testing, prototyping,
/// or sharing state between workers in a single process.
///
/// # Example
///
/// ```rust
/// # use apalis_core::backend::shared::MakeShared;
/// # use apalis_core::task::Task;
/// # use apalis_core::worker::context::WorkerContext;
/// # use apalis_core::worker::builder::WorkerBuilder;
/// # use apalis_core::backend::json::SharedJsonStore;
/// # use apalis_core::error::BoxDynError;
/// # use std::time::Duration;
/// # use apalis_core::backend::TaskSink;
///
/// #[tokio::main]
/// async fn main() {
///     let mut store = SharedJsonStore::new();
///     let mut int_store = store.make_shared().unwrap();
///     int_store.push(42).await.unwrap();
///
///     async fn task(
///         task: u32,
///         ctx: WorkerContext,
///     ) -> Result<(), BoxDynError> {
///         tokio::time::sleep(Duration::from_millis(2)).await;
///         ctx.stop()?;
///         Ok(())
///     }
///
///     let int_worker = WorkerBuilder::new("int-worker")
///         .backend(int_store)
///         .build(task)
///         .run();
///
///     int_worker.await.unwrap();
/// }
/// ```
///
/// See the tests for more advanced usage with multiple types and event listeners.
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_channel::mpsc::SendError;
use futures_core::Stream;
use futures_sink::Sink;
use futures_util::{SinkExt, StreamExt};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{
    backend::{
        impls::json::{
            JsonStorage,
            meta::JsonMapMetadata,
            util::{FindFirstWith, TaskKey, TaskWithMeta},
        },
        impls::memory::{MemorySink, MemoryStorage},
    },
    task::{Task, status::Status, task_id::TaskId},
};

#[derive(Debug)]
struct SharedJsonStream<T, Ctx> {
    inner: JsonStorage<Value>,
    req_type: std::marker::PhantomData<(T, Ctx)>,
}

impl<Args: DeserializeOwned + Unpin> Stream for SharedJsonStream<Args, JsonMapMetadata> {
    type Item = Task<Args, JsonMapMetadata>;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use crate::task::builder::TaskBuilder;
        let map = self.inner.tasks.try_read().expect("Failed to read tasks");
        if let Some((key, _)) = map.find_first_with(|k, _| {
            &k.namespace == std::any::type_name::<Args>() && k.status == Status::Pending
        }) {
            let task = map.get(&key).unwrap();
            let args = match Args::deserialize(&task.args) {
                Ok(value) => value,
                Err(_) => return Poll::Pending,
            };
            let task = TaskBuilder::new(args)
                .with_task_id(key.task_id.clone())
                .with_ctx(task.ctx.clone())
                .build();
            let key = key.clone();
            drop(map);
            let this = &mut self.get_mut().inner;
            this.update_status(&key, Status::Running)
                .expect("Failed to update status");
            this.persist_to_disk().expect("Failed to persist to disk");
            Poll::Ready(Some(task))
        } else {
            Poll::Pending
        }
    }
}
/// Sharable JSON based backend.
///
/// # Features
///
/// - Concurrent processing of multiple task types
/// - In-memory storage with optional disk persistence
/// - Metadata support for tasks
#[derive(Debug)]
pub struct SharedJsonStore {
    inner: JsonStorage<serde_json::Value>,
}

impl SharedJsonStore {
    /// Create a new instance of the shared JSON store.
    pub fn new() -> Self {
        Self {
            inner: JsonStorage::new_temp().unwrap(),
        }
    }
}

impl<Args: Send + Serialize + DeserializeOwned + Unpin + 'static>
    crate::backend::shared::MakeShared<Args> for SharedJsonStore
{
    type Backend = MemoryStorage<Args, JsonMapMetadata>;

    type Config = ();

    type MakeError = String;

    fn make_shared_with_config(
        &mut self,
        _: Self::Config,
    ) -> Result<Self::Backend, Self::MakeError> {
        let (sender, receiver) = self.inner.create_channel::<Args>();
        Ok(MemoryStorage {
            sender: MemorySink {
                inner: Arc::new(futures_util::lock::Mutex::new(sender)),
            },
            receiver,
        })
    }
}

impl JsonStorage<Value> {
    fn create_channel<Args: 'static + DeserializeOwned + Serialize + Send + Unpin>(
        &self,
    ) -> (
        Box<
            dyn Sink<Task<Args, JsonMapMetadata>, Error = SendError>
                + Send
                + Sync
                + Unpin
                + 'static,
        >,
        Pin<Box<dyn Stream<Item = Task<Args, JsonMapMetadata>> + Send>>,
    ) {
        // Create a channel for communication
        let sender = self.clone();

        // Create a wrapped sender that will insert into the in-memory store
        let wrapped_sender = {
            let mut store = self.clone();

            sender.with_flat_map(move |task: Task<Args, JsonMapMetadata>| {
                use crate::task::task_id::RandomId;
                let task_id = task
                    .parts
                    .task_id
                    .clone()
                    .unwrap_or(TaskId::new(RandomId::default()));
                let task = task.map(|args| serde_json::to_value(args).unwrap());
                store
                    .insert(
                        &TaskKey {
                            task_id,
                            namespace: std::any::type_name::<Args>().to_owned(),
                            status: Status::Pending,
                        },
                        TaskWithMeta {
                            args: task.args.clone(),
                            ctx: task.parts.ctx.clone(),
                            result: None,
                        },
                    )
                    .unwrap();
                futures_util::stream::iter(vec![Ok(task)])
            })
        };

        // Create a stream that filters by type T
        let filtered_stream = {
            let inner = self.clone();
            SharedJsonStream {
                inner,
                req_type: std::marker::PhantomData,
            }
        };

        // Combine the sender and receiver
        let sender = Box::new(wrapped_sender)
            as Box<dyn Sink<Task<Args, JsonMapMetadata>, Error = SendError> + Send + Sync + Unpin>;
        let receiver = filtered_stream.boxed();

        (sender, receiver)
    }
}
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::error::BoxDynError;

    use crate::worker::context::WorkerContext;
    use crate::{
        backend::{TaskSink, shared::MakeShared},
        worker::{builder::WorkerBuilder, ext::event_listener::EventListenerExt},
    };

    use super::*;

    const ITEMS: u32 = 10;

    #[tokio::test]
    async fn basic_shared() {
        let mut store = SharedJsonStore::new();
        let mut string_store = store.make_shared().unwrap();
        let mut int_store = store.make_shared().unwrap();
        for i in 0..ITEMS {
            string_store.push(format!("ITEM: {i}")).await.unwrap();
            int_store.push(i).await.unwrap();
        }

        async fn task(task: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_millis(2)).await;
            if task == ITEMS - 1 {
                ctx.stop()?;
                return Err("Worker stopped!")?;
            }
            Ok(())
        }

        let string_worker = WorkerBuilder::new("rango-tango-string")
            .backend(string_store)
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
            })
            .build(|req: String, ctx: WorkerContext| async move {
                tokio::time::sleep(Duration::from_millis(2)).await;
                println!("{req}");
                if req.ends_with(&(ITEMS - 1).to_string()) {
                    ctx.stop().unwrap();
                }
            })
            .run();

        let int_worker = WorkerBuilder::new("rango-tango-int")
            .backend(int_store)
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
            })
            .build(task)
            .run();

        let _ = futures_util::future::join(int_worker, string_worker).await;
    }
}
