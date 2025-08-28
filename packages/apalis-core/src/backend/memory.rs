#[cfg(feature = "json")]
use crate::backend::BackendWithCodec;
#[cfg(feature = "json")]
use crate::backend::{codec::json::JsonCodec, TaskResult};
#[cfg(feature = "json")]
use crate::worker::ext::ack::AcknowledgeLayer;
use crate::{
    backend::{Backend, TaskStream, WaitForCompletion},
    error::BoxDynError,
    task::{
        task_id::{RandomId, TaskId},
        Task,
    },
    worker::context::WorkerContext,
};
use crate::{task::extensions::Extensions, worker::ext::ack::Acknowledge};
use futures_channel::mpsc::{unbounded, SendError};
use futures_core::{future::BoxFuture, ready};
use futures_sink::Sink;
use futures_util::{
    future::{ready, Ready},
    stream::{self, BoxStream},
    FutureExt, SinkExt, Stream, StreamExt,
};
use serde::de::Error;
use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tower_layer::Identity;

#[cfg(feature = "serde")]
use serde::{de::DeserializeOwned, Serialize};
#[cfg(feature = "json")]
use serde_json::Value;
#[cfg(feature = "json")]
type JsonMapMetadata = serde_json::Map<String, serde_json::Value>;

#[cfg(feature = "json")]
impl<T> crate::task::metadata::MetadataExt<T> for JsonMapMetadata
where
    T: Serialize + DeserializeOwned,
{
    type Error = serde_json::Error;

    fn extract(&self) -> Result<T, serde_json::Error> {
        use serde::de::Error as _;
        let key = std::any::type_name::<T>();
        match self.get(key) {
            Some(value) => T::deserialize(value),
            None => Err(serde_json::Error::custom(format!(
                "No entry for type `{}` in metadata",
                key
            ))),
        }
    }

    fn inject(&mut self, value: T) -> Result<(), serde_json::Error> {
        let key = std::any::type_name::<T>();
        let json_value = serde_json::to_value(value)?;
        self.insert(key.to_string(), json_value);
        Ok(())
    }
}

#[derive(Debug, Clone)]
/// An example of the basics of a backend
pub struct MemoryStorage<S> {
    /// This would be the backend you are targeting, eg a connection poll
    inner: S,
}
impl<Args: Send + 'static> MemoryStorage<MemoryWrapper<Args>> {
    /// Create a new in-memory storage
    pub fn new() -> Self {
        let (sender, receiver) = unbounded();
        let sender = Box::new(sender)
            as Box<dyn Sink<Task<Args, Extensions>, Error = SendError> + Send + Sync + Unpin>;
        Self {
            inner: MemoryWrapper {
                sender: MemorySink {
                    inner: Arc::new(futures_util::lock::Mutex::new(sender)),
                },
                receiver: receiver.boxed(),
            },
        }
    }

    #[cfg(feature = "json")]
    pub fn new_with_json() -> MemoryStorage<JsonMemory<Args>> {
        MemoryStorage {
            inner: JsonMemory {
                tasks: Default::default(),
                buffer: Default::default(),
                results: Default::default(),
            },
        }
    }
}

impl<Args, Meta, T: Sink<Task<Args, Meta>> + Unpin + Send> Sink<Task<Args, Meta>>
    for MemoryStorage<T>
{
    type Error = T::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().inner.poll_ready_unpin(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Task<Args, Meta>) -> Result<(), Self::Error> {
        self.as_mut().inner.start_send_unpin(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().inner.poll_flush_unpin(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().inner.poll_close_unpin(cx)
    }
}

impl<Args, Meta> Sink<Task<Args, Meta>> for MemoryWrapper<Args, Meta> {
    type Error = SendError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().sender.poll_ready_unpin(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Task<Args, Meta>) -> Result<(), Self::Error> {
        self.as_mut().sender.start_send_unpin(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().sender.poll_flush_unpin(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.as_mut().sender.poll_close_unpin(cx)
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
struct TaskKey {
    task_id: TaskId,
    namespace: String,
}

#[cfg(feature = "json")]
#[derive(Debug, Clone)]
pub struct TaskWithMeta {
    args: Value,
    meta: JsonMapMetadata,
}

#[cfg(feature = "json")]
#[derive(Debug, Default)]
pub struct JsonMemory<Args> {
    tasks: std::sync::Arc<std::sync::RwLock<BTreeMap<TaskKey, std::sync::Mutex<TaskWithMeta>>>>,
    buffer: Vec<Task<Args, JsonMapMetadata>>,
    results: std::sync::Arc<futures_util::lock::Mutex<BTreeMap<TaskKey, Value>>>,
}

#[cfg(feature = "json")]
impl<Args> Clone for JsonMemory<Args> {
    fn clone(&self) -> Self {
        Self {
            tasks: self.tasks.clone(),
            buffer: Vec::new(),
            results: self.results.clone(),
        }
    }
}

#[cfg(feature = "json")]
impl JsonMemory<Value> {
    fn create_channel<Args: 'static + DeserializeOwned + Serialize + Send>(
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
            let inner = self.clone();

            sender.with_flat_map(move |request: Task<Args, JsonMapMetadata>| {
                use crate::task::task_id::RandomId;
                let task_id = request
                    .ctx
                    .task_id
                    .clone()
                    .unwrap_or(TaskId::new(RandomId::default()));
                let value = serde_json::to_value(request.args).unwrap();
                inner.tasks.write().unwrap().insert(
                    TaskKey {
                        task_id,
                        namespace: std::any::type_name::<Args>().to_owned(),
                    },
                    TaskWithMeta {
                        args: value.clone(),
                        meta: request.ctx.metadata.clone(),
                    }
                    .into(),
                );

                let req = Task::new_with_ctx(value, request.ctx);
                futures_util::stream::iter(vec![Ok(req)])
            })
        };

        // Create a stream that filters by type T
        let filtered_stream = {
            let inner = self.clone();
            SharedInMemoryStream {
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

#[cfg(feature = "json")]

impl<Args: Unpin + Serialize> Sink<Task<Args, JsonMapMetadata>> for JsonMemory<Args> {
    type Error = SendError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        item: Task<Args, JsonMapMetadata>,
    ) -> Result<(), Self::Error> {
        let this = Pin::get_mut(self);

        this.buffer.push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = Pin::get_mut(self);
        let mut tasks = this.tasks.write().unwrap();
        for task in this.buffer.drain(..) {
            use crate::task::task_id::RandomId;

            let task_id = task
                .ctx
                .task_id
                .clone()
                .unwrap_or(TaskId::new(RandomId::default()));
            tasks.insert(
                TaskKey {
                    task_id,
                    namespace: std::any::type_name::<Args>().to_owned(),
                },
                std::sync::Mutex::new(TaskWithMeta {
                    args: serde_json::to_value(task.args).unwrap(),
                    meta: task.ctx.metadata,
                }),
            );
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::<Task<Args, JsonMapMetadata>>::poll_flush(self, cx)
    }
}

#[cfg(feature = "json")]

impl<Args: DeserializeOwned> Stream for JsonMemory<Args> {
    type Item = Task<Args, JsonMapMetadata>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut map = self.tasks.write().unwrap();
        if let Some((key, mutex)) =
            map.pop_first_with(|s, _| s.namespace == std::any::type_name::<Args>())
        {
            use crate::task::builder::TaskBuilder;

            let task = mutex.into_inner().unwrap();
            Poll::Ready(Some(
                TaskBuilder::new_with_metadata(
                    serde_json::from_value(task.args).unwrap(),
                    task.meta,
                )
                .with_task_id(key.task_id)
                .build(),
            ))
        } else {
            Poll::Pending
        }
    }
}

pub struct MemorySink<Args, Meta = Extensions> {
    inner: Arc<
        futures_util::lock::Mutex<
            Box<dyn Sink<Task<Args, Meta>, Error = SendError> + Send + Sync + Unpin + 'static>,
        >,
    >,
}

impl<Args, Meta> Clone for MemorySink<Args, Meta> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<Args, Meta> Sink<Task<Args, Meta>> for MemorySink<Args, Meta> {
    type Error = SendError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut lock = ready!(self.inner.lock().poll_unpin(cx));
        Pin::new(&mut *lock).poll_ready_unpin(cx)
    }

    fn start_send(self: Pin<&mut Self>, mut item: Task<Args, Meta>) -> Result<(), Self::Error> {
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

/// In-memory queue that implements [Stream]
pub struct MemoryWrapper<Args, Meta = Extensions> {
    sender: MemorySink<Args, Meta>,
    receiver: Pin<Box<dyn Stream<Item = Task<Args, Meta>> + Send>>,
}

impl<Args, Meta> Stream for MemoryWrapper<Args, Meta> {
    type Item = Task<Args, Meta>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.as_mut().receiver.poll_next_unpin(cx)
    }
}

// MemoryStorage as a Backend
impl<Args: 'static + Clone + Send, Meta: 'static + Default> Backend<Args>
    for MemoryStorage<MemoryWrapper<Args, Meta>>
{
    type IdType = RandomId;

    type Meta = Meta;

    type Error = SendError;
    type Stream = TaskStream<Task<Args, Meta>, SendError>;
    type Layer = Identity;
    type Beat = BoxStream<'static, Result<(), Self::Error>>;

    fn heartbeat(&self, _: &WorkerContext) -> Self::Beat {
        stream::once(async { Ok(()) }).boxed()
    }
    fn middleware(&self) -> Self::Layer {
        Identity::new()
    }

    fn poll(self, _worker: &WorkerContext) -> Self::Stream {
        let stream = self.inner.map(|r| Ok(Some(r))).boxed();
        stream
    }
}

#[cfg(feature = "json")]
impl<Args: 'static + Send + DeserializeOwned> Backend<Args> for MemoryStorage<JsonMemory<Args>> {
    type IdType = RandomId;
    type Error = SendError;
    type Meta = JsonMapMetadata;
    type Stream = TaskStream<Task<Args, JsonMapMetadata>, SendError>;
    type Layer = AcknowledgeLayer<JsonAck<Args>>;
    type Beat = BoxStream<'static, Result<(), Self::Error>>;

    fn heartbeat(&self, _: &WorkerContext) -> Self::Beat {
        stream::once(async { Ok(()) }).boxed()
    }
    fn middleware(&self) -> Self::Layer {
        use crate::worker::ext::ack::AcknowledgeLayer;

        AcknowledgeLayer::new(JsonAck {
            inner: self.inner.clone(),
        })
    }
    fn poll(self, _worker: &WorkerContext) -> Self::Stream {
        let stream = self.inner.map(|r| Ok(Some(r))).boxed();
        stream
    }
}

#[cfg(feature = "json")]
pub struct JsonAck<Args> {
    inner: JsonMemory<Args>,
}

impl<Args> Clone for JsonAck<Args> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[cfg(feature = "json")]
impl<Args, Res: Serialize, Meta: Sync> Acknowledge<Res, Meta, RandomId> for JsonAck<Args> {
    type Error = serde_json::Error;

    type Future = BoxFuture<'static, Result<(), Self::Error>>;

    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        ctx: &crate::task::ExecutionContext<Meta, RandomId>,
    ) -> Self::Future {
        let mutex = self.inner.results.clone();
        let val = serde_json::to_value(res.as_ref().map_err(|e| e.to_string())).unwrap();
        let task_id = ctx.task_id.clone().unwrap();
        async move {
            let mut tasks = mutex.lock().await;

            tasks.insert(
                TaskKey {
                    task_id ,
                    namespace: std::any::type_name::<Args>().to_owned(),
                },
                val,
            );
            Ok(())
        }
        .boxed()
    }
}

#[cfg(feature = "json")]
impl<Res: 'static + DeserializeOwned + Send, Compact: 'static> WaitForCompletion<Res, Compact>
    for MemoryStorage<JsonMemory<Compact>>
where
    Compact: Send + DeserializeOwned + 'static,
{
    type ResultStream = BoxStream<'static, Result<TaskResult<Res>, SendError>>;
    fn wait_for(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>>,
    ) -> Self::ResultStream {
        use std::{collections::HashSet, time::Duration};

        let task_ids: HashSet<_> = task_ids.into_iter().collect();
        struct PollState<T> {
            vault: Arc<futures_util::lock::Mutex<BTreeMap<TaskKey, serde_json::Value>>>,
            pending_tasks: HashSet<TaskId>,
            namespace: String,
            poll_interval: Duration,
            _phantom: std::marker::PhantomData<T>,
        }
        let state = PollState {
            vault: self.inner.results.clone(),
            pending_tasks: task_ids,
            namespace: std::any::type_name::<Compact>().to_owned(),
            poll_interval: Duration::from_millis(100),
            _phantom: std::marker::PhantomData,
        };
        stream::unfold(state, |mut state: PollState<Res>| {
            async move {
                // panic!( "{}", state.pending_tasks.len());
                // If no pending tasks, we're done
                if state.pending_tasks.is_empty() {
                    return None;
                }

                loop {
                    // Check for completed tasks
                    let vault = state.vault.lock().await;
                    let completed_task = state.pending_tasks.iter().find_map(|task_id| {
                        let key = TaskKey {
                            task_id: task_id.clone(),
                            namespace: state.namespace.clone(),
                        };

                        vault.get(&key).map(|value| {
                            use crate::backend::TaskResult;

                            let result = match serde_json::from_value::<Result<Res, String>>(
                                value.clone(),
                            ) {
                                Ok(result) => TaskResult {
                                    task_id: task_id.clone(),
                                    result,
                                },
                                Err(e) => TaskResult {
                                    task_id: task_id.clone(),
                                    result: Err(format!("Deserialization error: {}", e)),
                                },
                            };
                            (task_id.clone(), result)
                        })
                    });
                    drop(vault);

                    if let Some((task_id, result)) = completed_task {
                        state.pending_tasks.remove(&task_id);
                        return Some((Ok(result), state));
                    }

                    // No completed tasks, wait and try again
                    crate::timer::sleep(state.poll_interval).await;
                }
            }
        })
        .boxed()
    }
}

#[cfg(feature = "json")]
impl<Args> BackendWithCodec for MemoryStorage<JsonMemory<Args>> {
    type Codec = JsonCodec<Value>;
    type Compact = Value;
}

trait PopFirstWith<K, V> {
    fn pop_first_with<F>(&mut self, predicate: F) -> Option<(K, V)>
    where
        F: FnMut(&K, &V) -> bool;
}

impl<K, V> PopFirstWith<K, V> for BTreeMap<K, V>
where
    K: Ord + Clone,
{
    fn pop_first_with<F>(&mut self, mut predicate: F) -> Option<(K, V)>
    where
        F: FnMut(&K, &V) -> bool,
    {
        if let Some(key) = self
            .iter()
            .find(|(k, v)| predicate(k, v))
            .map(|(k, _)| k.clone())
        {
            self.remove_entry(&key)
        } else {
            None
        }
    }
}

#[cfg(feature = "json")]
#[derive(Debug)]
struct SharedInMemoryStream<T, Meta> {
    inner: JsonMemory<Value>,
    req_type: std::marker::PhantomData<(T, Meta)>,
}

#[cfg(feature = "json")]
impl<Args: DeserializeOwned> Stream for SharedInMemoryStream<Args, JsonMapMetadata> {
    type Item = Task<Args, JsonMapMetadata>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use crate::task::builder::TaskBuilder;
        let mut map = self.inner.tasks.write().unwrap();
        if let Some((key, mutex)) =
            map.pop_first_with(|k, v| &k.namespace == std::any::type_name::<Args>())
        {
            let task = mutex.into_inner().unwrap();
            let args = match serde_json::from_value::<Args>(task.args) {
                Ok(value) => value,
                Err(_) => return Poll::Ready(None),
            };
            Poll::Ready(Some(
                TaskBuilder::new_with_metadata(args, task.meta)
                    .with_task_id(key.task_id)
                    .build(),
            ))
        } else {
            Poll::Ready(None)
        }
    }
}

#[cfg(feature = "json")]
#[derive(Debug, Default)]
pub struct SharedJsonMemory {
    inner: JsonMemory<serde_json::Value>,
}

#[cfg(feature = "json")]
impl<Args: Send + Serialize + DeserializeOwned + 'static> crate::backend::shared::MakeShared<Args>
    for SharedJsonMemory
{
    type Backend = MemoryStorage<MemoryWrapper<Args, JsonMapMetadata>>;

    type Config = ();

    type MakeError = String;

    fn make_shared_with_config(
        &mut self,
        _: Self::Config,
    ) -> Result<Self::Backend, Self::MakeError> {
        let (sender, receiver) = self.inner.create_channel::<Args>();
        Ok(MemoryStorage {
            inner: MemoryWrapper {
                sender: MemorySink {
                    inner: Arc::new(futures_util::lock::Mutex::new(sender)),
                },
                receiver,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Deref, sync::atomic::AtomicUsize, time::Duration};

    use futures_util::future::ready;

    use crate::{
        backend::{memory::MemoryStorage, shared::MakeShared, TaskSink},
        service_fn::{self, service_fn, ServiceFn},
        task::data::Data,
        worker::{
            builder::WorkerBuilder,
            ext::{
                ack::AcknowledgementExt, circuit_breaker::CircuitBreaker,
                event_listener::EventListenerExt, long_running::LongRunningExt,
            },
        },
    };

    use super::*;

    const ITEMS: u32 = 100;

    #[tokio::test]
    async fn it_works() {
        let mut store = SharedJsonMemory::default();
        let mut string_store = store.make_shared().unwrap();
        let mut int_store = store.make_shared().unwrap();
        for i in 0..ITEMS {
            string_store.push(format!("ITEM: {i}")).await.unwrap();
            int_store.push(i).await.unwrap();
        }
        #[derive(Clone, Debug, Default)]
        struct Count(Arc<AtomicUsize>);

        impl Deref for Count {
            type Target = Arc<AtomicUsize>;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        async fn task(job: u32, count: Data<Count>, ctx: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_millis(2)).await;
            if job == ITEMS - 1 {
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
            .data(Count::default())
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
            })
            .build(task)
            .run();

        let _ = futures_util::future::join(int_worker, string_worker).await;
    }
}
