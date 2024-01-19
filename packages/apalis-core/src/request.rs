use futures::{future::BoxFuture, Stream, StreamExt};
use serde::{Deserialize, Serialize};

use std::{fmt::Debug, pin::Pin, sync::Arc};

use crate::{
    data::Extensions,
    error::{Error, StreamError},
    notify::Notify,
    poller::{controller::Control, Ready},
    worker::Worker,
};

/// Represents a job which can be serialized and executed

#[derive(Serialize, Debug, Deserialize, Clone)]
pub struct Request<T> {
    pub req: T,
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

pub type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;

/// Represents a result for a future that yields T
pub type RequestFuture<T> = BoxFuture<'static, T>;
/// Represents a stream for T.
pub type RequestStream<T> = BoxStream<'static, Result<Option<T>, Error>>;

pub struct RequestStreamPoll<T> {
    pub stream: RequestStream<T>,
    pub notify: Notify<Worker<Ready<T>>>,
    pub controller: Control,
}

impl<T> RequestStreamPoll<T> {
    pub fn new(stream: RequestStream<T>) -> Self {
        Self {
            stream,
            notify: Notify::new(),
            controller: Control::new(),
        }
    }
}
