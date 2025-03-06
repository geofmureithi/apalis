use std::{
    any::type_name,
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, Mutex, RwLock},
};

use futures::{future::BoxFuture, FutureExt, Stream};
use serde::{Deserialize, Serialize};

use crate::{
    codec::Codec,
    error::Error,
    notify::Notify,
    poller::Poller,
    request::{BoxStream, Request, RequestStream, State},
    response::Response,
    worker::{Context, Worker},
};

/// A backend represents a task source
/// Both [`Storage`] and [`MessageQueue`] need to implement it for workers to be able to consume tasks
///
/// [`Storage`]: crate::storage::Storage
/// [`MessageQueue`]: crate::mq::MessageQueue
pub trait Backend<Req> {
    /// The stream to be produced by the backend
    type Stream: Stream<Item = Result<Option<Req>, crate::error::Error>>;

    /// Returns the final decoration of layers
    type Layer;

    /// The way data is stored in the backend
    type Compact;

    /// Returns a poller that is ready for streaming
    fn poll(self, worker: &Worker<Context>) -> Poller<Self::Stream, Self::Layer>;
}

pub struct Shared<Backend, Context, Codec: crate::codec::Codec, Config> {
    pub instances: Arc<
        RwLock<HashMap<String, Notify<Result<Option<Request<Codec::Compact, Context>>, Error>>>>,
    >,
    pub ack: Notify<(Context, Response<Codec::Compact>)>,
    pub backend: Arc<Backend>,
    pub config: PhantomData<Config>,
}

impl<Backend, Context, Codec, Config> Shared<Backend, Context, Codec, Config>
where
    Codec: crate::codec::Codec,
{
    pub fn new(backend: Backend) -> Self {
        Self {
            instances: Arc::default(),
            ack: Notify::new(),
            backend: Arc::new(backend),
            config: PhantomData,
        }
    }

    pub fn make_shared<B>(&mut self) -> Result<B, Backend::MakeError>
    where
        Backend: Sharable<B, Codec, Context = Context, Config = Config>,
        Backend::Config: Default,
    {
        Backend::share(self)
    }

    pub fn make_shared_with_config<B>(&mut self, config: Config) -> Result<B, Backend::MakeError>
    where
        Backend: Sharable<B, Codec, Context = Context, Config = Config>,
    {
        Backend::share_with_config(self, config)
    }
}

// #[derive(Debug)]
#[non_exhaustive]
pub enum BackendConnection<Compact, Context> {
    StandAlone {
        subscription: Notify<()>,
    },
    Shared {
        receiver: Notify<Result<Option<Request<Compact, Context>>, Error>>,
    },
}

impl<Compact, Context> Clone for BackendConnection<Compact, Context> {
    fn clone(&self) -> Self {
        match self {
            BackendConnection::Shared { receiver } => BackendConnection::Shared {
                receiver: receiver.clone(),
            },
            BackendConnection::StandAlone { subscription } => BackendConnection::StandAlone {
                subscription: subscription.clone(),
            },
        }
    }
}

/// Trait to make multiple backends using a shared connection
/// This can improve performance for example sql engines since it leads to only one query for multiple job types
pub trait Sharable<Backend, Codec: crate::codec::Codec>: Sized {
    type Context;
    /// The Config for the backend
    type Config;
    /// The error returned if the backend cant be shared
    type MakeError;

    /// Returns the backend to be shared
    fn share(
        parent: &mut Shared<Self, Self::Context, Codec, Self::Config>,
    ) -> Result<Backend, Self::MakeError>;

    /// Returns the backend with config
    fn share_with_config(
        parent: &mut Shared<Self, Self::Context, Codec, Self::Config>,
        config: Self::Config,
    ) -> Result<Backend, Self::MakeError>;
}

pub trait SharedPolling {
    type Output: Future<Output = ()>;

    fn polling(&self) -> Self::Output;
}

// /// A generic connection wrapper that allows backend to be shareable
// // #[derive(Debug)]
// pub enum BackendConnection<Compact, Context> {
//     Shared {
//         /// The ack notification
//         ack: Notify<(Context, Response<Compact>)>,
//         /// The shared receiver
//         receiver: Notify<Request<Compact, Context>>,
//         /// The shared poller that drives the connection.
//         /// When all workers are dead, polling stops
//         poller: Shared<BoxFuture<'static, ()>>,
//     },
//     Single {
//         /// The ack notification
//         ack: Notify<(Context, Response<Compact>)>,
//         /// This allows you to provide a stream that triggers the next poll
//         ticker: Arc<futures::lock::Mutex<RequestStream<()>>>,
//     },
// }

// impl<Compact> std::fmt::Debug for BackendConnection<Compact> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             Connection::Shared { .. } => f
//                 .debug_struct("Shared")
//                 .field("ack", &"Notify<(SqlContext, Response<Compact>)>")
//                 .field("receiver", &"Notify<Request<Args, SqlContext>>")
//                 .field("poller", &"<future>")
//                 .finish(),
//             Connection::Single { .. } => f
//                 .debug_struct("Single")
//                 .field("ack", &"Notify<(SqlContext, Response<Compact>)>")
//                 .field("ticker", &"RequestStream<()>")
//                 .finish(),
//         }
//     }
// }

// impl<Compact, Context> Clone for BackendConnection<Compact, Context> {
//     fn clone(&self) -> Self {
//         match self {
//             BackendConnection::Shared {
//                 ack,
//                 receiver,
//                 poller,
//             } => BackendConnection::Shared {
//                 ack: ack.clone(),
//                 receiver: receiver.clone(),
//                 poller: poller.clone(),
//             },
//             BackendConnection::Single { ack, ticker } => BackendConnection::Single {
//                 ack: ack.clone(),
//                 ticker: ticker.clone(),
//             },
//         }
//     }
// }

/// Represents functionality that allows reading of jobs and stats from a backend
/// Some backends esp MessageQueues may not currently implement this
pub trait BackendExpose<T>
where
    Self: Sized,
{
    /// The request type being handled by the backend
    type Request;
    /// The error returned during reading jobs and stats
    type Error;
    /// List all Workers that are working on a backend
    fn list_workers(
        &self,
    ) -> impl Future<Output = Result<Vec<Worker<WorkerState>>, Self::Error>> + Send;

    /// Returns the counts of jobs in different states
    fn stats(&self) -> impl Future<Output = Result<Stat, Self::Error>> + Send;

    /// Fetch jobs persisted in a backend
    fn list_jobs(
        &self,
        status: &State,
        page: i32,
    ) -> impl Future<Output = Result<Vec<Self::Request>, Self::Error>> + Send;
}

/// Represents the current statistics of a backend
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct Stat {
    /// Represents pending tasks
    pub pending: usize,
    /// Represents running tasks
    pub running: usize,
    /// Represents dead tasks
    pub dead: usize,
    /// Represents failed tasks
    pub failed: usize,
    /// Represents successful tasks
    pub success: usize,
}

/// A serializable version of a worker's state.
#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerState {
    /// Type of task being consumed by the worker, useful for display and filtering
    pub r#type: String,
    /// The type of job stream
    pub source: String,
    // TODO: // The layers that were loaded for worker.
    // TODO: // pub layers: Vec<Layer>,
    // TODO: // last_seen: Timestamp,
}
impl WorkerState {
    /// Build a new state
    pub fn new<S>(r#type: String) -> Self {
        Self {
            r#type,
            source: type_name::<S>().to_string(),
        }
    }
}
