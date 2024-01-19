#![crate_name = "apalis_core"]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub,
    bad_style,
    dead_code,
    improper_ctypes,
    non_shorthand_field_patterns,
    no_mangle_generic_items,
    overflowing_literals,
    path_statements,
    patterns_in_fns_without_body,
    unconditional_recursion,
    unused,
    unused_allocation,
    unused_comparisons,
    unused_parens,
    while_true
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! # apalis-core
//! Utilities for building job and message processing tools.
use executor::Executor;
use futures::{Future, StreamExt};
use monitor::shutdown::Shutdown;
use notify::Notify;
use poller::{controller::Control, stream::BackendStream, Ready};
use request::{Request, RequestStream, RequestStreamPoll};
use std::fmt;
use std::sync::Arc;
pub use tower::{layer::layer_fn, util::BoxCloneService, Layer, Service, ServiceBuilder};
use worker::{Worker, WorkerId};

/// Represent utilities for creating worker instances.
pub mod builder;
/// Includes all possible error types.
pub mod error;

/// Represents middleware offered through [`tower::Layer`]
pub mod layers;
/// Represents the job bytes.
pub mod request;
/// Represents different possible responses.
pub mod response;
/// Represents a service that is created from a function.
pub mod service_fn;

/// Represents ability to persist and consume jobs from storages.
pub mod storage;

/// Represents an executor. Currently tokio is implemented as default
pub mod executor;
/// Represents monitoring of running workers
pub mod monitor;
/// Represents extra utils needed for runtime agnostic approach
pub mod utils;
/// Represents the utils for building workers.
pub mod worker;

/// Represents the utils needed to extend a task's context.
pub mod data;
/// Message queuing utilities
pub mod mq;
/// Controlled polling and streaming
pub mod poller;

/// Allows async listening in a mpsc style.
pub mod notify;

/// A generic layer that has been stripped off types.
/// This is returned by a [Backend] and can be used to customize the middleware of the service consuming tasks
pub struct CommonLayer<In, T, U, E> {
    boxed: Arc<dyn Layer<In, Service = BoxCloneService<T, U, E>> + Send + Sync + 'static>,
}

impl<In, T, U, E> CommonLayer<In, T, U, E> {
    /// Create a new [`CommonLayer`].
    pub fn new<L>(inner_layer: L) -> Self
    where
        L: Layer<In> + Send + Sync + 'static,
        L::Service: Service<T, Response = U, Error = E> + Send + 'static + Clone,
        <L::Service as Service<T>>::Future: Send + 'static,
    {
        let layer = layer_fn(move |inner: In| {
            let out = inner_layer.layer(inner);
            BoxCloneService::new(out)
        });

        Self {
            boxed: Arc::new(layer),
        }
    }
}

impl<In, T, U, E> Layer<In> for CommonLayer<In, T, U, E> {
    type Service = BoxCloneService<T, U, E>;

    fn layer(&self, inner: In) -> Self::Service {
        self.boxed.layer(inner)
    }
}

impl<In, T, U, E> Clone for CommonLayer<In, T, U, E> {
    fn clone(&self) -> Self {
        Self {
            boxed: Arc::clone(&self.boxed),
        }
    }
}

impl<In, T, U, E> fmt::Debug for CommonLayer<In, T, U, E> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("CommonLayer").finish()
    }
}
/// A backend represents a task source
/// Both [crate::storage::Storage] and [crate::mq::MessageQueue] need to implement it for workers to be able to consume tasks
pub trait Backend<Req> {
    /// The type in which the backend stores its requests
    /// Usually it can be [`Vec<u8>`], a row type in sql.
    type Compact;
    /// Adds the ability for the backend to define a [Codec]
    type Codec;

    type Notifier: Clone;

    type Controller;

    fn notifier(&self) -> &Self::Notifier;

    fn controller(&self) -> &Self::Controller;

    /// The codec for the backend
    fn codec(&self) -> &Self::Codec;
    /// Allows the backend to decorate the service with [Layer]
    fn common_layer<S>(&self, worker: &WorkerId) -> CommonLayer<S, Req, S::Response, S::Error>
    where
        S: Service<Req> + Send + 'static + Clone,
        S::Future: Send + 'static,
    {
        let builder = ServiceBuilder::new();
        CommonLayer::new(builder)
    }

    fn poll(self, worker: WorkerId) -> impl std::future::Future<Output = ()> + Send;
}

impl<Req: Sync + Send + 'static> Backend<Request<Req>> for RequestStreamPoll<Request<Req>> {
    fn codec(&self) -> &Self::Codec {
        &()
    }
    type Compact = ();
    type Codec = ();
    type Controller = Control;
    type Notifier = Notify<Worker<Ready<Request<Req>>>>;
    fn notifier(&self) -> &Self::Notifier {
        &self.notify
    }
    fn controller(&self) -> &Self::Controller {
        &self.controller
    }

    fn poll(self, worker: WorkerId) -> impl std::future::Future<Output = ()> + Send {
        let mut notify = self.notifier().clone();
        let mut stream = BackendStream::new(self.stream, self.controller);

        async move {
            while let Some(mut poll) = notify.next().await {
                let fut = stream.next();
                poll.send(fut.await.unwrap().unwrap().unwrap()).unwrap();
            }
        }
    }
}

/// In-Memory utilities
pub mod memory {
    use crate::{
        mq::MessageQueue,
        notify::Notify,
        poller::{controller::Control, stream::BackendStream, Ready},
        request::{Request, RequestStream},
        worker::{Worker, WorkerId},
        Backend,
    };
    use futures::{
        channel::mpsc::{channel, Receiver, Sender},
        Stream, StreamExt,
    };
    use std::{
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    };

    #[derive(Debug)]
    /// An example of the basics of a backend
    pub struct MemoryStorage<T> {
        /// Required for workers to inform the backend they are ready to consume
        notifier: Notify<Worker<Ready<T>>>,
        /// Required for [Poller] to control polling.
        controller: Control,
        /// This would be the backend you are targeting, eg a connection poll
        inner: MemoryWrapper<T>,
    }
    impl<T> MemoryStorage<Request<T>> {
        /// Create a new in-memory storage
        pub fn new() -> Self {
            Self {
                notifier: Notify::new(),
                controller: Control::new(),
                inner: MemoryWrapper::new(),
            }
        }
    }

    impl<T> Default for MemoryStorage<Request<T>> {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<T> Clone for MemoryStorage<T> {
        fn clone(&self) -> Self {
            Self {
                controller: self.controller.clone(),
                notifier: self.notifier.clone(),
                inner: self.inner.clone(),
            }
        }
    }

    /// In-memory queue that implements [Stream]
    #[derive(Debug)]
    pub struct MemoryWrapper<T> {
        sender: Sender<T>,
        receiver: Arc<futures::lock::Mutex<Receiver<T>>>,
    }

    impl<T> Clone for MemoryWrapper<T> {
        fn clone(&self) -> Self {
            Self {
                receiver: self.receiver.clone(),
                sender: self.sender.clone(),
            }
        }
    }

    impl<T> MemoryWrapper<T> {
        /// Build a new basic queue channel
        pub fn new() -> Self {
            let (sender, receiver) = channel(100);

            Self {
                sender,
                receiver: Arc::new(futures::lock::Mutex::new(receiver)),
            }
        }
    }

    impl<T> Default for MemoryWrapper<T> {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<T> Stream for MemoryWrapper<T> {
        type Item = T;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if let Some(mut receiver) = self.receiver.try_lock() {
                receiver.poll_next_unpin(cx)
            } else {
                Poll::Pending
            }
        }
    }

    // MemoryStorage as a Backend
    impl<T: Send + 'static + Sync> Backend<Request<T>> for MemoryStorage<Request<T>> {
        type Compact = ();
        type Codec = ();
        type Controller = Control;

        type Notifier = Notify<Worker<Ready<Request<T>>>>;
        fn codec(&self) -> &Self::Codec {
            todo!()
        }
        fn controller(&self) -> &Self::Controller {
            &self.controller
        }
        fn notifier(&self) -> &Self::Notifier {
            &self.notifier
        }

        fn poll(self, worker: WorkerId) -> impl std::future::Future<Output = ()> + Send {
            let mut notify = self.notifier().clone();
            let mut stream = BackendStream::new(self.inner, self.controller);

            async move {
                while let Some(mut poll) = notify.next().await {
                    let fut = stream.next();
                    poll.send(fut.await.unwrap()).unwrap();
                }
            }
        }
    }

    impl<Message: Send + 'static + Sync> MessageQueue<Message> for MemoryStorage<Request<Message>> {
        type Error = ();
        async fn enqueue(&self, message: Message) -> Result<(), Self::Error> {
            self.inner
                .sender
                .clone()
                .try_send(Request::new(message))
                .unwrap();
            Ok(())
        }

        async fn dequeue(&self) -> Result<Option<Message>, ()> {
            Ok(Some(
                self.inner.receiver.lock().await.next().await.unwrap().req,
            ))
        }

        async fn size(&self) -> Result<usize, ()> {
            Ok(self.inner.clone().count().await)
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TokioTestExecutor;

impl Executor for TokioTestExecutor {
    fn spawn(&self, _future: impl Future<Output = ()> + Send + 'static) {
        #[cfg(test)]
        tokio::spawn(_future);
    }
}
