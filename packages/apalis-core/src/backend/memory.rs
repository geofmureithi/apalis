use crate::{
    backend::{
        codec::{CloneOpCodec, Encoder},
        shared::MakeShared,
        Backend, RequestStream, TaskSink,
    },
    error::BoxDynError,
    request::{task_id::TaskId, Parts, Request},
    worker::{self, context::WorkerContext},
};
use futures::{
    channel::mpsc::{channel, unbounded, Receiver, SendError, Sender},
    executor::block_on,
    future::pending,
    stream::{self, BoxStream},
    FutureExt, Sink, SinkExt, Stream, StreamExt,
};
use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, Mutex, RwLock},
    task::{Context, Poll},
    time::Duration,
};
use tower::layer::util::Identity;

#[cfg(feature = "serde")]
use serde::{de::DeserializeOwned, Serialize};
#[cfg(feature = "json")]
use serde_json::Value;
#[derive(Debug, Clone)]
/// An example of the basics of a backend
pub struct MemoryStorage<S> {
    /// This would be the backend you are targeting, eg a connection poll
    inner: S,
}
impl<T: Send + 'static> MemoryStorage<MemoryWrapper<T>> {
    /// Create a new in-memory storage
    pub fn new() -> Self {
        let (sender, receiver) = unbounded();
        let sender = Box::new(sender)
            as Box<dyn Sink<Request<T, ()>, Error = SendError> + Send + Sync + Unpin>;
        Self {
            inner: MemoryWrapper {
                sender: MemorySink {
                    inner: Arc::new(RwLock::new(sender)),
                },
                receiver: receiver.boxed(),
            },
        }
    }

    #[cfg(feature = "json")]
    pub fn new_with_json() -> MemoryStorage<JsonMemory<T>> {
        MemoryStorage {
            inner: JsonMemory {
                tasks: Default::default(),
                buffer: Default::default(),
            },
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
struct TaskKey {
    task_id: TaskId,
    namespace: String,
}

#[cfg(feature = "json")]
#[derive(Debug, Clone, Default)]
pub struct JsonMemory<T> {
    tasks: Arc<RwLock<BTreeMap<TaskKey, Mutex<Value>>>>,
    buffer: Vec<Request<T, ()>>,
}

#[cfg(feature = "json")]
impl JsonMemory<Value> {
    fn create_channel<T: 'static + DeserializeOwned + Serialize + Send>(
        &self,
    ) -> (
        Box<dyn Sink<Request<T, ()>, Error = SendError> + Send + Sync + Unpin + 'static>,
        Pin<Box<dyn Stream<Item = Request<T, ()>> + Send>>,
    ) {
        // Create a channel for communication
        let sender = self.clone();

        // Create a wrapped sender that will insert into the in-memory store
        let wrapped_sender = {
            let inner = self.clone();

            sender.with_flat_map(move |request: Request<T, ()>| {
                let task_id = request.parts.task_id.clone();
                let value = serde_json::to_value(request.args).unwrap();
                inner.tasks.write().unwrap().insert(
                    TaskKey {
                        task_id,
                        namespace: std::any::type_name::<T>().to_owned(),
                    },
                    value.clone().into(),
                );

                let req = Request::new_with_parts(value, request.parts);
                futures::stream::iter(vec![Ok(req)])
            })
        };

        // Create a stream that filters by type T
        let filtered_stream = {
            let inner = self.clone();
            SharedInMemoryStream {
                inner,
                req_type: PhantomData,
            }
        };

        // Combine the sender and receiver
        let sender = Box::new(wrapped_sender)
            as Box<dyn Sink<Request<T, ()>, Error = SendError> + Send + Sync + Unpin>;
        let receiver = filtered_stream.boxed();

        (sender, receiver)
    }
}

#[cfg(feature = "json")]

impl<T: Unpin + Serialize> Sink<Request<T, ()>> for JsonMemory<T> {
    type Error = SendError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Request<T, ()>) -> Result<(), Self::Error> {
        let this = Pin::get_mut(self);

        this.buffer.push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = Pin::get_mut(self);
        let mut tasks = this.tasks.write().unwrap();
        for task in this.buffer.drain(..) {
            let task_id = task.parts.task_id.clone();
            tasks.insert(
                TaskKey {
                    task_id,
                    namespace: std::any::type_name::<T>().to_owned(),
                },
                Mutex::new(serde_json::to_value(task.args).unwrap()),
            );
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::<Request<T, ()>>::poll_flush(self, cx)
    }
}

#[cfg(feature = "json")]

impl<T: DeserializeOwned> Stream for JsonMemory<T> {
    type Item = Request<T, ()>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut map = self.tasks.write().unwrap();
        if let Some((key, mutex)) =
            map.pop_first_with(|s, _| s.namespace == std::any::type_name::<T>())
        {
            let args = mutex.into_inner().unwrap();
            Poll::Ready(Some(Request::new_with_parts(
                serde_json::from_value(args).unwrap(),
                Parts {
                    task_id: key.task_id,
                    ..Default::default()
                },
            )))
        } else {
            Poll::Pending
        }
    }
}

pub struct MemorySink<T> {
    inner: Arc<
        RwLock<Box<dyn Sink<Request<T, ()>, Error = SendError> + Send + Sync + Unpin + 'static>>,
    >,
}

impl<T> Clone for MemorySink<T> {
    fn clone(&self) -> Self {
        MemorySink {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Sink<Request<T, ()>> for MemorySink<T> {
    type Error = SendError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut lock = self.inner.write().unwrap();
        Pin::new(&mut *lock).poll_ready_unpin(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Request<T, ()>) -> Result<(), Self::Error> {
        let mut lock = self.inner.write().unwrap();
        Pin::new(&mut *lock).start_send_unpin(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut lock = self.inner.write().unwrap();
        Pin::new(&mut *lock).poll_flush_unpin(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut lock = self.inner.write().unwrap();
        Pin::new(&mut *lock).poll_close_unpin(cx)
    }
}

impl<T: Clone + Send + Unpin> TaskSink<T> for MemorySink<T> {
    type Codec = CloneOpCodec;

    type Error = SendError;

    type Compact = T;

    type Context = ();

    type Timestamp = u64;

    async fn push_raw_request(
        &mut self,
        req: Request<Self::Compact, Self::Context>,
    ) -> Result<Parts<Self::Context>, Self::Error> {
        let p = req.parts.clone();
        let mut sink = self.inner.write().unwrap();
        let req = sink.send(req).boxed();
        block_on(req.map(|s| s.map(|_| p)))
    }
}

#[cfg(feature = "json")]
impl<T: Serialize + Send + Unpin> TaskSink<T> for JsonMemory<T>
// where
//     JsonMemory<T>: Sink<Request<Value, ()>>,
{
    type Error = serde_json::Error;

    type Codec = super::codec::json::JsonCodec<Value>;

    type Compact = Value;

    type Context = ();

    type Timestamp = u64;
    async fn push_raw_request(
        &mut self,
        request: Request<Self::Compact, Self::Context>,
    ) -> Result<Parts<Self::Context>, Self::Error> {
        let task_id = request.parts.task_id.clone();
        let value = serde_json::to_value(request.args).unwrap();
        let parts = request.parts;
        self.tasks.write().unwrap().insert(
            TaskKey {
                task_id,
                namespace: std::any::type_name::<T>().to_owned(),
            },
            value.into(),
        );
        Ok(parts)
    }
}

/// In-memory queue that implements [Stream]
pub struct MemoryWrapper<T> {
    sender: MemorySink<T>,
    receiver: Pin<Box<dyn Stream<Item = Request<T, ()>> + Send>>,
}

impl<T> Stream for MemoryWrapper<T> {
    type Item = Request<T, ()>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.as_mut().receiver.poll_next_unpin(cx)
    }
}

// MemoryStorage as a Backend
impl<T: 'static + Clone + Send> Backend<T, ()> for MemoryStorage<MemoryWrapper<T>> {
    type Error = BoxDynError;
    type Stream = RequestStream<Request<T, ()>>;
    type Layer = Identity;
    type Beat = BoxStream<'static, Result<(), Self::Error>>;
    type Sink = MemorySink<T>;

    fn heartbeat(&self, _: &WorkerContext) -> Self::Beat {
        stream::once(async { Ok(()) }).boxed()
    }
    fn middleware(&self) -> Self::Layer {
        Identity::new()
    }

    fn sink(&self) -> Self::Sink {
        self.inner.sender.clone()
    }

    fn poll(self, _worker: &WorkerContext) -> Self::Stream {
        let stream = self.inner.map(|r| Ok(Some(r))).boxed();
        stream
    }
}

#[cfg(feature = "json")]
impl<T: 'static + Clone + Send + DeserializeOwned> Backend<T, ()> for MemoryStorage<JsonMemory<T>> {
    type Error = BoxDynError;
    type Stream = RequestStream<Request<T, ()>>;
    type Layer = Identity;
    type Beat = BoxStream<'static, Result<(), Self::Error>>;
    type Sink = JsonMemory<T>;

    fn heartbeat(&self, _: &WorkerContext) -> Self::Beat {
        stream::once(async { Ok(()) }).boxed()
    }
    fn middleware(&self) -> Self::Layer {
        Identity::new()
    }

    fn sink(&self) -> Self::Sink {
        self.inner.clone()
    }

    fn poll(self, _worker: &WorkerContext) -> Self::Stream {
        let stream = self.inner.map(|r| Ok(Some(r))).boxed();
        stream
    }
}

#[cfg(feature = "json")]
#[derive(Debug, Clone, Default)]
struct Wrapped {
    namespace: String,
    value: serde_json::Value,
}

pub trait PopFirstWith<K, V> {
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
struct SharedInMemoryStream<T> {
    inner: JsonMemory<Value>,
    req_type: PhantomData<T>,
}

#[cfg(feature = "json")]
impl<T: DeserializeOwned> Stream for SharedInMemoryStream<T> {
    type Item = Request<T, ()>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut map = self.inner.tasks.write().unwrap();
        if let Some((key, mutex)) =
            map.pop_first_with(|k, v| &k.namespace == std::any::type_name::<T>())
        {
            let args = mutex.into_inner().unwrap();
            let args = match serde_json::from_value::<T>(args) {
                Ok(value) => value,
                Err(_) => return Poll::Ready(None),
            };
            Poll::Ready(Some(Request::new_with_parts(
                args,
                Parts {
                    task_id: key.task_id,
                    ..Default::default()
                },
            )))
        } else {
            Poll::Ready(None)
        }
    }
}

#[cfg(feature = "json")]
impl<Args: Send + Serialize + DeserializeOwned + 'static> MakeShared<Args> for JsonMemory<Value> {
    type Backend = MemoryStorage<MemoryWrapper<Args>>;

    type Config = ();

    type MakeError = String;

    fn make_shared_with_config(
        &mut self,
        _: Self::Config,
    ) -> Result<Self::Backend, Self::MakeError> {
        let (sender, receiver) = self.create_channel();
        Ok(MemoryStorage {
            inner: MemoryWrapper {
                sender: MemorySink {
                    inner: Arc::new(RwLock::new(sender)),
                },
                receiver,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Deref, sync::atomic::AtomicUsize, time::Duration};

    use futures::future::ready;

    use crate::{
        backend::memory::MemoryStorage,
        backend::TaskSink,
        request::data::Data,
        service_fn::{self, service_fn, ServiceFn},
        worker::builder::WorkerBuilder,
        worker::ext::{
            ack::AcknowledgementExt, circuit_breaker::CircuitBreaker,
            event_listener::EventListenerExt, long_running::LongRunningExt,
        },
    };

    use super::*;

    const ITEMS: u32 = 100;

    #[tokio::test]
    async fn it_works() {
        let mut store = JsonMemory::default();
        let string_store = store.make_shared().unwrap();
        let int_store = store.make_shared().unwrap();
        let mut int_sink = int_store.sink();
        let mut string_sink = string_store.sink();

        for i in 0..ITEMS {
            string_sink.push(format!("ITEM: {i}")).await.unwrap();
            int_sink.push(i).await.unwrap();
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
                ctx.stop();
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
                    ctx.stop();
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

        let _ = futures::future::join(int_worker, string_worker).await;
    }
}
