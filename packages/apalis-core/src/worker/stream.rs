use futures::{Future, Stream, StreamExt};
use std::pin::Pin;
use std::process::Output;
use std::task::{Context, Poll};

use super::WorkerNotify;

// Define your struct
pub struct WorkerStream<T, S>
where
    S: Stream<Item = T>,
{
    notify: WorkerNotify<T>,
    stream: S,
}

impl<T, S> WorkerStream<T, S>
where
    S: Stream<Item = T> + Unpin + 'static,
{
    pub fn new(stream: S, notify: WorkerNotify<T>) -> Self {
        Self { notify, stream }
    }
    pub fn to_future(mut self) -> impl Future<Output = ()> {
        Box::pin(async move {
            loop {
                self.next().await;
            }
        })
    }
}

impl<T, S> Stream for WorkerStream<T, S>
where
    S: Stream<Item = T> + Unpin,
{
    type Item = (); // Modify according to your needs

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Poll for the next listener
        match this.notify.poll_next_unpin(cx) {
            Poll::Ready(Some(mut worker)) => {
                // Once we have a listener, poll the inner stream
                match this.stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(item)) => {
                        // Send the item
                        if let Err(_e) = worker.send(item) {
                            // Handle the error, e.g., the worker is dropped
                        }
                        Poll::Ready(Some(()))
                    }
                    Poll::Ready(None) => Poll::Ready(None), // Inner stream is exhausted
                    Poll::Pending => Poll::Pending,
                }
            }
            Poll::Ready(None) => Poll::Ready(None), // No more workers
            Poll::Pending => Poll::Pending,
        }
    }
}
