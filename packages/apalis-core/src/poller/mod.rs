use futures::{future::BoxFuture, Future, FutureExt};
use std::fmt::{self, Debug};
use tower::layer::util::Identity;

/// Util for controlling pollers
pub mod controller;
/// Util for controlled stream
pub mod stream;

/// A poller type that allows fetching from a stream and a heartbeat future that can be used to do periodic tasks
pub struct Poller<S, L = Identity> {
    /// The stream of jobs
    pub stream: S,
    /// The heartbeat for the backend
    pub heartbeat: BoxFuture<'static, ()>,
    /// The tower middleware provided by the backend
    pub layer: L,
    pub(crate) _priv: (),
}

impl<S> Poller<S, Identity> {
    /// Build a new poller
    pub fn new(stream: S, heartbeat: impl Future<Output = ()> + Send + 'static) -> Self {
        Self::new_with_layer(stream, heartbeat, Identity::new())
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
            _priv: (),
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
