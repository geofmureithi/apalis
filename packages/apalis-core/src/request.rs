use futures::{future::BoxFuture, Stream};
use serde::{Deserialize, Serialize};
use tower::{layer::util::Identity, ServiceBuilder};

use std::{fmt::Debug, pin::Pin};

use crate::{data::Extensions, error::Error, poller::Poller, worker::WorkerId, Backend};

/// Represents a job which can be serialized and executed

#[derive(Serialize, Debug, Deserialize, Clone)]
pub struct Request<T> {
    pub(crate) req: T,
    #[serde(skip)]
    pub(crate) data: Extensions,
}

impl<T> Request<T> {
    /// Creates a new [Request]
    pub fn new(req: T) -> Self {
        Self {
            req,
            data: Extensions::new(),
        }
    }

    /// Creates a request with context provided
    pub fn new_with_data(req: T, data: Extensions) -> Self {
        Self { req, data }
    }

    /// Get the underlying reference of the request
    pub fn inner(&self) -> &T {
        &self.req
    }

    /// Take the underlying reference of the request
    pub fn take(self) -> T {
        self.req
    }
}

impl<T> std::ops::Deref for Request<T> {
    type Target = Extensions;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> std::ops::DerefMut for Request<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

/// Represents a stream that is send
pub type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;

/// Represents a result for a future that yields T
pub type RequestFuture<T> = BoxFuture<'static, T>;
/// Represents a stream for T.
pub type RequestStream<T> = BoxStream<'static, Result<Option<T>, Error>>;

impl<T> Backend<Request<T>> for RequestStream<Request<T>> {
    type Stream = Self;

    type Layer = ServiceBuilder<Identity>;

    fn common_layer(&self, _worker: WorkerId) -> Self::Layer {
        ServiceBuilder::new()
    }

    fn poll(self, _worker: WorkerId) -> Poller<Self::Stream> {
        Poller {
            stream: self,
            heartbeat: Box::pin(async {}),
        }
    }
}
