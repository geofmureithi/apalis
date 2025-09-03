use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_channel::mpsc::SendError;
use futures_core::{stream::BoxStream, Stream};
use futures_util::{stream, StreamExt};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{
    backend::{
        codec::json::JsonCodec,
        impls::json::{
            meta::JsonMapMetadata,
            util::{FindFirstWith, JsonAck},
            JsonStorage,
        },
        Backend, TaskStream,
    },
    task::{status::Status, task_id::RandomId, Task},
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};

impl<Args: 'static + Send + DeserializeOwned + Unpin> Backend<Args> for JsonStorage<Args> {
    type IdType = RandomId;
    type Error = SendError;
    type Ctx = JsonMapMetadata;
    type Stream = TaskStream<Task<Args, JsonMapMetadata>, SendError>;
    type Layer = AcknowledgeLayer<JsonAck<Args>>;
    type Beat = BoxStream<'static, Result<(), Self::Error>>;

    type Codec = JsonCodec<Value>;

    fn heartbeat(&self, _: &WorkerContext) -> Self::Beat {
        stream::once(async { Ok(()) }).boxed()
    }
    fn middleware(&self) -> Self::Layer {
        AcknowledgeLayer::new(JsonAck {
            inner: self.clone(),
        })
    }
    fn poll(self, _worker: &WorkerContext) -> Self::Stream {
        let stream = self.map(|r| Ok(Some(r))).boxed();
        stream
    }
}
impl<Args: DeserializeOwned + Unpin> Stream for JsonStorage<Args> {
    type Item = Task<Args, JsonMapMetadata>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let map = self.tasks.try_write().unwrap();
        if let Some((key, task)) = map.find_first_with(|s, _| {
            s.namespace == std::any::type_name::<Args>() && s.status == Status::Pending
        }) {
            use crate::task::builder::TaskBuilder;
            let key = key.clone();
            let args = Args::deserialize(&task.args).unwrap();
            let task = TaskBuilder::new(args)
                .with_task_id(key.task_id.clone())
                .with_ctx(task.ctx.clone())
                .build();
            drop(map);
            let this = self.get_mut();
            this.update_status(&key, Status::Running)
                .expect("Failed to update status");
            this.persist_to_disk().expect("Failed to persist to disk");
            Poll::Ready(Some(task))
        } else {
            Poll::Pending
        }
    }
}
