use std::{
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    task::{Context, Poll},
};

use futures::{stream::FusedStream, Stream, StreamExt};
use pin_project_lite::pin_project;

use super::{controller::Control, STOPPED};

// Macro for pin projection used in `BackendStream`.
pin_project! {
    /// `BackendStream` is a wrapper around another stream `S`.
    /// It controls the flow of the stream based on the `Controller` state.
    #[derive(Debug, Clone)]
    pub struct BackendStream<S> {
        #[pin]
        state: State<S>,
        controller: Control,
    }
}

pin_project! {
    /// `State` is a part of `BackendStream` that holds the actual stream `S`.
    #[project = StateProj]
    #[derive(Debug, Clone)]
    pub(crate) struct State<S> {
        #[pin]
        pub stream: S,
    }
}

impl<S> BackendStream<S> {
    /// Creates a new `BackendStream` from a given stream and a shared `Controller`.
    pub fn new(stream: S, controller: Control) -> Self {
        BackendStream {
            state: State { stream },
            controller,
        }
    }
}
impl<S: Stream + Unpin> Stream for BackendStream<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.controller.is_plugged() {
            self.state.stream.poll_next_unpin(cx)
        } else {
            Poll::Pending
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.state.stream.size_hint()
    }
}

impl<S: Stream + Unpin> FusedStream for BackendStream<S> {
    fn is_terminated(&self) -> bool {
        self.controller.state.load(Ordering::SeqCst) == STOPPED
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::{self, StreamExt};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::time::{self, Duration};
    use tokio_stream::wrappers::IntervalStream;

    fn mock_stream() -> impl Stream<Item = i32> {
        stream::iter(vec![1, 2, 3])
    }

    fn interval_stream(duration: Duration) -> IntervalStream {
        IntervalStream::new(time::interval(duration))
    }

    #[test]
    fn test_backend_stream_plugged() {
        let controller = Control::new();
        controller.plug();
        let mut backend_stream = BackendStream::new(mock_stream(), controller);

        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        match Pin::new(&mut backend_stream).poll_next(&mut context) {
            Poll::Ready(Some(item)) => assert_eq!(item, 1),
            _ => panic!("Expected item from stream"),
        }
    }

    #[test]
    fn test_backend_stream_unplugged() {
        let controller = Control::new();
        controller.unplug();
        let mut backend_stream = BackendStream::new(mock_stream(), controller);

        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        match Pin::new(&mut backend_stream).poll_next(&mut context) {
            Poll::Pending => (),
            _ => panic!("Expected Poll::Pending"),
        }
    }

    #[test]
    fn test_backend_stream_plug_unplug() {
        let controller = Control::new();
        controller.unplug();
        let mut backend_stream = BackendStream::new(mock_stream(), controller.clone());

        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        match Pin::new(&mut backend_stream).poll_next(&mut context) {
            Poll::Pending => (),
            _ => panic!("Expected Poll::Pending"),
        };
        controller.plug();

        match Pin::new(&mut backend_stream).poll_next(&mut context) {
            Poll::Ready(Some(item)) => assert_eq!(item, 1),
            _ => panic!("Expected item from stream"),
        }
        controller.unplug();
        match Pin::new(&mut backend_stream).poll_next(&mut context) {
            Poll::Pending => (),
            _ => panic!("Expected Poll::Pending"),
        };
        controller.plug();

        match Pin::new(&mut backend_stream).poll_next(&mut context) {
            Poll::Ready(Some(item)) => assert_eq!(item, 2),
            _ => panic!("Expected item from stream"),
        }
    }

    // Test that BackendStream polls items from an interval stream when plugged
    #[tokio::test]
    async fn test_backend_stream_with_interval_plugged() {
        let controller = Control::new();
        controller.plug();
        let mut backend_stream =
            BackendStream::new(interval_stream(Duration::from_millis(100)), controller);

        // Polling the stream should yield an item
        backend_stream
            .next()
            .await
            .expect("Expected an item from the stream");
    }

    #[tokio::test]
    async fn test_backend_stream_with_interval_unplugged() {
        let controller = Control::new();
        controller.unplug();
        let mut backend_stream =
            BackendStream::new(interval_stream(Duration::from_millis(100)), controller);

        // Using tokio::time::timeout to ensure that the stream doesn't yield an item
        match tokio::time::timeout(Duration::from_millis(200), backend_stream.next()).await {
            Ok(None) | Err(_) => (), // Expected as stream is unplugged
            _ => panic!("Expected no item from the stream"),
        }
    }

    #[tokio::test]
    async fn test_backend_stream_interval_plug_unplug() {
        let controller = Control::new();
        controller.unplug();
        let mut backend_stream = BackendStream::new(
            interval_stream(Duration::from_millis(100)),
            controller.clone(),
        );

        // Using tokio::time::timeout to ensure that the stream doesn't yield an item
        match tokio::time::timeout(Duration::from_millis(200), backend_stream.next()).await {
            Err(_) => (),
            _ => panic!("Expected no item from the stream"),
        }
        controller.plug();
        backend_stream
            .next()
            .await
            .expect("Expected an item from the stream");
    }
}
