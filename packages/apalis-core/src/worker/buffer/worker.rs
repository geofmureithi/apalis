use super::{
    error::{Closed, ServiceError},
    message::Message,
};
use futures::{channel::mpsc, ready, Stream};
use std::sync::{Arc, Mutex};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use tower::Service;

pin_project_lite::pin_project! {
    #[derive(Debug)]
    pub struct Worker<T, Request>
    where
        T: Service<Request>,
    {
        current_message: Option<Message<Request, T::Future>>,
        rx: mpsc::Receiver<Message<Request, T::Future>>,
        service: T,
        finish: bool,
        failed: Option<ServiceError>,
        handle: Handle,
    }
}

/// Get the error out
#[derive(Debug)]
pub(crate) struct Handle {
    inner: Arc<Mutex<Option<ServiceError>>>,
}

impl<T, Request> Worker<T, Request>
where
    T: Service<Request>,
    T::Error: Into<tower::BoxError>,
{
    pub(crate) fn new(
        service: T,
        rx: mpsc::Receiver<Message<Request, T::Future>>,
    ) -> (Handle, Worker<T, Request>) {
        let handle = Handle {
            inner: Arc::new(Mutex::new(None)),
        };

        let worker = Worker {
            current_message: None,
            finish: false,
            failed: None,
            rx,
            service,
            handle: handle.clone(),
        };

        (handle, worker)
    }

    /// Return the next queued Message that hasn't been canceled.
    ///
    /// If a `Message` is returned, the `bool` is true if this is the first time we received this
    /// message, and false otherwise (i.e., we tried to forward it to the backing service before).
    fn poll_next_msg(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<(Message<Request, T::Future>, bool)>> {
        if self.finish {
            // We've already received None and are shutting down
            return Poll::Ready(None);
        }

        // tracing::trace!("worker polling for next message");
        if let Some(msg) = self.current_message.take() {
            // If the oneshot sender is closed, then the receiver is dropped,
            // and nobody cares about the response. If this is the case, we
            // should continue to the next request.
            if !msg.tx.is_canceled() {
                // tracing::trace!("resuming buffered request");
                return Poll::Ready(Some((msg, false)));
            }

            // tracing::trace!("dropping cancelled buffered request");
        }

        // Get the next request
        while let Some(msg) = ready!(Pin::new(&mut self.rx).poll_next(cx)) {
            if !msg.tx.is_canceled() {
                // tracing::trace!("processing new request");
                return Poll::Ready(Some((msg, true)));
            }
            // Otherwise, request is canceled, so pop the next one.
            // tracing::trace!("dropping cancelled request");
        }

        Poll::Ready(None)
    }

    fn failed(&mut self, error: tower::BoxError) {
        let error = ServiceError::new(error);

        let mut inner = self.handle.inner.lock().unwrap();

        if inner.is_some() {
            return;
        }

        *inner = Some(error.clone());
        drop(inner);

        self.rx.close();
        self.failed = Some(error);
    }
}

impl<T, Request> Future for Worker<T, Request>
where
    T: Service<Request>,
    T::Error: Into<tower::BoxError>,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.finish {
            return Poll::Ready(());
        }

        loop {
            match ready!(self.poll_next_msg(cx)) {
                Some((msg, _)) => {
                    if let Some(ref failed) = self.failed {
                        let _ = msg.tx.send(Err(failed.clone()));
                        continue;
                    }
                    match self.service.poll_ready(cx) {
                        Poll::Ready(Ok(())) => {
                            let response = self.service.call(msg.request);
                            let _ = msg.tx.send(Ok(response));
                        }
                        Poll::Pending => {
                            self.current_message = Some(msg);
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(e)) => {
                            let error = e.into();
                            self.failed(error);
                            let _ = msg.tx.send(Err(self
                                .failed
                                .as_ref()
                                .expect("Worker::failed did not set self.failed?")
                                .clone()));
                        }
                    }
                }
                None => {
                    // No more more requests _ever_.
                    self.finish = true;
                    return Poll::Ready(());
                }
            }
        }
    }
}

impl Handle {
    pub(crate) fn get_error_on_closed(&self) -> tower::BoxError {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|svc_err| svc_err.clone().into())
            .unwrap_or_else(|| Closed::new().into())
    }
}

impl Clone for Handle {
    fn clone(&self) -> Handle {
        Handle {
            inner: self.inner.clone(),
        }
    }
}
