use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_channel::mpsc::SendError;
use futures_sink::Sink;
use serde::Serialize;
use serde_json::Value;

use crate::{
    backend::impls::json::{
        JsonStorage,
        meta::JsonMapMetadata,
        util::{TaskKey, TaskWithMeta},
    },
    task::{Task, task_id::TaskId},
};

impl<Args: Unpin + Serialize> Sink<Task<Value, JsonMapMetadata>> for JsonStorage<Args> {
    type Error = SendError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        item: Task<Value, JsonMapMetadata>,
    ) -> Result<(), Self::Error> {
        let this = Pin::get_mut(self);

        this.buffer.push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = Pin::get_mut(self);
        let tasks: Vec<_> = this.buffer.drain(..).collect();
        for task in tasks {
            use crate::task::task_id::RandomId;

            let task_id = task
                .parts
                .task_id
                .clone()
                .unwrap_or(TaskId::new(RandomId::default()));

            let key = TaskKey {
                task_id,
                queue: std::any::type_name::<Args>().to_owned(),
                status: crate::task::status::Status::Pending,
            };
            this.insert(
                &key,
                TaskWithMeta {
                    args: task.args,
                    ctx: task.parts.ctx,
                    result: None,
                },
            )
            .unwrap();
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Sink::<Task<Value, JsonMapMetadata>>::poll_flush(self, cx)
    }
}
