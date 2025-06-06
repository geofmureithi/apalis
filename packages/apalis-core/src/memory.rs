use crate::{
    backend::{Backend, Push, Schedule},
    error::BoxDynError,
    request::{Parts, Request, RequestStream},
    shared::MakeShared,
    task::task_id::TaskId,
    worker::{self, WorkerContext},
};
use futures::{
    channel::mpsc::{channel, Receiver, Sender},
    stream::{self, BoxStream},
    Sink, SinkExt, Stream, StreamExt,
};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    pin::Pin,
    sync::{Arc, Mutex, RwLock},
    task::{Context, Poll},
    time::Duration,
};
use tower::layer::util::Identity;

#[derive(Debug)]
/// An example of the basics of a backend
pub struct MemoryStorage<S> {
    /// This would be the backend you are targeting, eg a connection poll
    inner: S,
}
// impl<T> MemoryStorage<MemoryWrapper<T>> {
//     /// Create a new in-memory storage
//     pub fn new() -> Self {
//         Self {
//             inner: MemoryWrapper::new(),
//         }
//     }
// }

impl<T: Default> Default for MemoryStorage<InMemory<T>> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

impl<T: Clone> Clone for MemoryStorage<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[derive(Clone, Default)]
pub struct InMemory<T> {
    tasks: Arc<RwLock<BTreeMap<TaskId, Mutex<T>>>>,
    buffer: Vec<(TaskId, T)>,
}
impl<Args> InMemory<Args> {
    fn insert(&self, task_id: TaskId, args: Args) {
        self.tasks.write().unwrap().insert(task_id, args.into());
    }
}

impl<T: Unpin> Sink<(TaskId, T)> for InMemory<T> {
    type Error = std::io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: (TaskId, T)) -> Result<(), Self::Error> {
        let this = Pin::get_mut(self);
        this.buffer.push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = Pin::get_mut(self);
        let mut tasks = this.tasks.write().unwrap();
        for (task_id, value) in this.buffer.drain(..) {
            tasks.insert(task_id, Mutex::new(value));
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}

impl<T> Stream for InMemory<T> {
    type Item = Request<T, ()>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut map = self.tasks.write().unwrap();

        if let Some((task_id, mutex)) = map.pop_first() {
            let args = mutex.into_inner().unwrap();
            Poll::Ready(Some(Request::new_with_parts(
                args,
                Parts {
                    task_id,
                    ..Default::default()
                },
            )))
        } else {
            Poll::Ready(None)
        }
    }
}

/// In-memory queue that implements [Stream]
pub struct MemoryWrapper<T> {
    sender: Box<dyn Sink<Request<T, ()>, Error = ()> + Send + Unpin + 'static>,
    receiver: Pin<Box<dyn Stream<Item = Request<T, ()>> + Send>>,
}

impl<T> Stream for MemoryWrapper<T> {
    type Item = Request<T, ()>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.as_mut().receiver.poll_next_unpin(cx)
    }
}

// MemoryStorage as a Backend
impl<S: Stream<Item = Request<T, ()>> + Send + 'static, T> Backend<Request<T, ()>>
    for MemoryStorage<S>
{
    type Error = BoxDynError;
    type Stream = RequestStream<Request<T, ()>>;
    type Layer = Identity;
    type Beat = BoxStream<'static, Result<(), Self::Error>>;
    type Codec = ();
    fn heartbeat(&self) -> Self::Beat {
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

impl<Args: Send + 'static> Push<Args, ()> for MemoryStorage<InMemory<Args>> {
    type Compact = Args;
    async fn push_request(&mut self, req: Request<Args, ()>) -> Result<Parts<()>, Self::Error> {
        self.push_raw_request(req).await
    }

    async fn push_raw_request(
        &mut self,
        req: Request<Self::Compact, ()>,
    ) -> Result<Parts<()>, Self::Error> {
        let parts = req.parts.clone();
        self.inner.insert(req.parts.task_id, req.args);
        Ok(parts)
    }
}

impl<Args: Send + 'static> Push<Args, ()> for MemoryStorage<MemoryWrapper<Args>> {
    type Compact = Args;
    async fn push_request(&mut self, req: Request<Args, ()>) -> Result<Parts<()>, Self::Error> {
        self.push_raw_request(req).await
    }

    async fn push_raw_request(
        &mut self,
        req: Request<Self::Compact, ()>,
    ) -> Result<Parts<()>, Self::Error> {
        let parts = req.parts.clone();
        self.inner.sender.send(req).await.unwrap();
        Ok(parts)
    }
}

impl<Args: Send + Sync + 'static> Schedule<Args, ()> for MemoryStorage<InMemory<Args>> {
    type Timestamp = Duration;
    async fn schedule_request(
        &mut self,
        req: Request<Args, ()>,
        on: Self::Timestamp,
    ) -> Result<Parts<()>, Self::Error> {
        // sleep(on).await;
        let parts = req.parts.clone();
        self.inner.insert(req.parts.task_id, req.args);
        Ok(parts)
    }

    async fn schedule_raw_request(
        &mut self,
        req: Request<Self::Compact, ()>,
        on: Self::Timestamp,
    ) -> Result<Parts<()>, Self::Error> {
        unreachable!("Requests must be typed")
    }
}

#[derive(Debug, Clone, Default)]
struct Wrapped {
    namespace: String,
    value: serde_json::Value,
}

#[derive(Default)]
pub struct SharedMemoryStorage {
    inner: InMemory<Wrapped>,
}
impl SharedMemoryStorage {
    fn create_channel<T: 'static + DeserializeOwned + Serialize>(
        &self,
    ) -> (
        Box<dyn Sink<Request<T, ()>, Error = ()> + Send + Unpin + 'static>,
        Pin<Box<dyn Stream<Item = Request<T, ()>> + Send>>,
    ) {
        // Create a channel for communication
        let sender = self.inner.clone();

        // Create a wrapped sender that will insert into the in-memory store
        let wrapped_sender = {
            let inner = self.inner.clone();

            sender
                .sink_map_err(|_| ())
                .with_flat_map(move |request: Request<T, ()>| {
                    let namespace = std::any::type_name::<T>().to_string();
                    let task_id = request.parts.task_id;
                    let value = serde_json::to_value(request.args).unwrap();

                    let wrapped = Wrapped { namespace, value };
                    inner.insert(task_id.clone(), wrapped.clone());

                    futures::stream::iter(vec![Ok((task_id, wrapped))])
                })
        };

        // Create a stream that filters by type T
        let filtered_stream = {
            let inner = self.inner.clone();

            inner.filter_map(move |req| async move {
                let expected_namespace = std::any::type_name::<T>().to_string();
                if req.args.namespace == expected_namespace {
                    match serde_json::from_value::<T>(req.args.value) {
                        Ok(value) => Some(Request::new_with_parts(value, req.parts)),
                        Err(_) => None,
                    }
                } else {
                    None
                }
            })
        };

        // Combine the sender and receiver
        let sender =
            Box::new(wrapped_sender) as Box<dyn Sink<Request<T, ()>, Error = ()> + Send + Unpin>;
        let receiver = filtered_stream.boxed();

        (sender, receiver)
    }
}

impl<T: Serialize + DeserializeOwned + 'static> MakeShared<Request<T, ()>> for SharedMemoryStorage {
    type Config = ();
    type MakeError = BoxDynError;
    type Backend = MemoryStorage<MemoryWrapper<T>>;
    fn make_shared(&mut self) -> Result<Self::Backend, Self::MakeError> {
        self.make_shared_with_config(Default::default())
    }

    fn make_shared_with_config(
        &mut self,
        _: Self::Config,
    ) -> Result<Self::Backend, Self::MakeError> {
        let (sender, receiver) = self.create_channel();
        Ok(MemoryStorage {
            inner: MemoryWrapper { sender, receiver },
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Deref, sync::atomic::AtomicUsize, time::Duration};

    use crate::{
        backend::Push,
        builder::{WorkerBuilder, WorkerFactory, WorkerFactoryFn},
        data::Data,
        ext::{
            ack::AcknowledgementExt, event_listener::EventListenerExt,
            long_running::LongRunningExt, record_attempt::RecordAttempt,
        },
        memory::MemoryStorage,
        service_fn::{self, service_fn, ServiceFn},
    };

    use super::*;

    const ITEMS: u32 = 1;

    #[tokio::test]
    async fn it_works() {
        let mut store = SharedMemoryStorage::default();
        let mut int_store = store.make_shared().unwrap();
        let mut string_store = store.make_shared().unwrap();

        for i in 0..ITEMS {
            int_store.push(i).await.unwrap();
        }
        for i in 0..ITEMS {
            string_store.push(format!("ITEM: {i}")).await.unwrap();
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
            tokio::time::sleep(Duration::from_secs(1)).await;
            if job == ITEMS - 1 {
                ctx.stop();
                return Err("Worker stopped!")?;
            }
            Ok(())
        }


        let int_worker = WorkerBuilder::new("rango-tango-int")
            .backend(int_store)
            .data(Count::default())
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {:?}", ctx.id, ev);
            })
            .build_fn(task).run().await.unwrap();

        // let string_worker = WorkerBuilder::new("rango-tango-string")
        //     .backend(string_store)
        //     .data(Count::default())
        //     .on_event(|ctx, ev| {
        //         println!("CTX {:?}, On Event = {:?}", ctx.id, ev);
        //     })
        //     .build_fn(|req: String, ctx: WorkerContext| async move {
        //         println!("{req}");
        //         ctx.stop();
        //     }).run();
        //     let _ = futures::future::select(string_worker, int_worker).await;
        //     let _ = tokio::join!(string_worker.run(), int_worker.run());
    }
}
