use futures::{future::BoxFuture, Future, FutureExt};
use std::{
    fmt::{self, Debug},
    ops::{Deref, DerefMut},
};
use tower::layer::util::Identity;

/// Util for controlling pollers
pub mod controller;
/// Util for controlled stream
pub mod stream;

/// A poller type that allows fetching from a stream and a heartbeat future that can be used to do periodic tasks
pub struct Poller<S, L = Identity> {
    pub(crate) stream: S,
    pub(crate) heartbeat: BoxFuture<'static, ()>,
    pub(crate) layer: L,
}

impl<S> Poller<S, Identity> {
    /// Build a new poller
    pub fn new(stream: S, heartbeat: impl Future<Output = ()> + Send + 'static) -> Self {
        Self {
            stream,
            heartbeat: heartbeat.boxed(),
            layer: Identity::new(),
        }
    }

    /// Build a poller with layer
    pub fn new_with_layer<L>(
        stream: S,
        heartbeat: impl Future<Output = ()> + Send + 'static,
        layer: L,
    ) -> Poller<S, L> {
        Poller {
            stream,
            heartbeat: heartbeat.boxed(),
            layer,
        }
    }
}

impl<S, L> Debug for Poller<S, L>
where
    S: Debug,
    L: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Poller")
            .field("stream", &self.stream)
            .field("heartbeat", &"...")
            .field("layer", &self.layer)
            .finish()
    }
}

const STOPPED: usize = 2;
const PLUGGED: usize = 1;
const UNPLUGGED: usize = 0;

/// Tells the poller that the worker is ready for a new request
#[derive(Debug)]
pub struct FetchNext<T> {
    sender: async_oneshot::Sender<T>,
}

impl<T> Deref for FetchNext<T> {
    type Target = async_oneshot::Sender<T>;
    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

impl<T> DerefMut for FetchNext<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.sender
    }
}
impl<T> FetchNext<T> {
    /// Generate a new instance of ready
    pub fn new(sender: async_oneshot::Sender<T>) -> Self {
        Self { sender }
    }
}
