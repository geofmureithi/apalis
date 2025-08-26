use crate::task::extensions::Extensions;
use crate::{
    backend::{Backend, TaskStream},
    error::BoxDynError,
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
        let key = std::any::type_name::<T>();
        match self.get(key) {
            Some(value) => serde_json::from_value(value.clone()),
            None => todo!(),
            // None => Err(serde_json::Error::custom(format!(
            //     "No entry for type `{}` in metadata",
            //     key
            // ))),
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

    fn start_send(
        mut self: Pin<&mut Self>,
        item: Task<Args, Meta>,
    ) -> Result<(), Self::Error> {
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
#[derive(Debug, Clone, Default)]
pub struct JsonMemory<Args> {
    tasks: std::sync::Arc<std::sync::RwLock<BTreeMap<TaskKey, std::sync::Mutex<Value>>>>,
    buffer: Vec<Task<Args, JsonMapMetadata>>,
}

#[cfg(feature = "json")]
impl JsonMemory<Value> {
    fn create_channel<Args: 'static + DeserializeOwned + Serialize + Send>(
        &self,
    ) -> (
        Box<dyn Sink<Task<Args, JsonMapMetadata>, Error = SendError> + Send + Sync + Unpin + 'static>,
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
                    value.clone().into(),
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
                std::sync::Mutex::new(serde_json::to_value(task.args).unwrap()),
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

            let args = mutex.into_inner().unwrap();
            Poll::Ready(Some(
                TaskBuilder::new(serde_json::from_value(args).unwrap())
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
            Box<
                dyn Sink<Task<Args, Meta>, Error = SendError> + Send + Sync + Unpin + 'static,
            >,
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

    fn start_send(
        self: Pin<&mut Self>,
        mut item: Task<Args, Meta>,
    ) -> Result<(), Self::Error> {
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
impl<Args: 'static + Clone + Send, Meta: 'static> Backend<Args, Meta>
    for MemoryStorage<MemoryWrapper<Args, Meta>>
{
    type IdType = RandomId;

    type Error = BoxDynError;
    type Stream = TaskStream<Task<Args, Meta>>;
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
impl<Args: 'static + Send + DeserializeOwned> Backend<Args, JsonMapMetadata>
    for MemoryStorage<JsonMemory<Args>>
{
    type IdType = RandomId;
    type Error = BoxDynError;
    type Stream = TaskStream<Task<Args, JsonMapMetadata>>;
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
impl<Args: DeserializeOwned, Meta: Default> Stream for SharedInMemoryStream<Args, Meta> {
    type Item = Task<Args, Meta>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use crate::task::builder::TaskBuilder;
        let mut map = self.inner.tasks.write().unwrap();
        if let Some((key, mutex)) =
            map.pop_first_with(|k, v| &k.namespace == std::any::type_name::<Args>())
        {
            let args = mutex.into_inner().unwrap();
            let args = match serde_json::from_value::<Args>(args) {
                Ok(value) => value,
                Err(_) => return Poll::Ready(None),
            };
            Poll::Ready(Some(
                TaskBuilder::new(args).with_task_id(key.task_id).build(),
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
