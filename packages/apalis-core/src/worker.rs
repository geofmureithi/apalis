use std::fmt::Debug;

use actix::prelude::*;
use futures::Future;
use serde::{de::DeserializeOwned, Serialize};
use tower::Service;

use crate::{
    error::JobError,
    queue::QueueError,
    queue::{Queue, QueueStatus},
    request::JobRequest,
    response::JobResult,
    storage::Storage,
};

#[derive(Message)]
#[rtype(result = "Result<QueueStatus, QueueError>")]
pub enum WorkerManagement {
    Status,
    Stop,
    Restart,
    Setup,
    Ack(String),
    Kill(String),
}

impl Worker {
    pub fn new() -> Self {
        Self { addrs: Vec::new() }
    }
    pub async fn run(self) -> std::io::Result<()> {
        actix_rt::signal::ctrl_c().await
    }
}

/// Represents a monitor for multiple instances of [Queue]
/// Keeps an address of each queue and periodically checks of their status
/// When combined with the `web` feature, it can be used to manage the queues from a web ui.
pub struct Worker {
    addrs: Vec<Recipient<WorkerManagement>>,
}

impl Worker {
    /// Register a single queue
    pub fn register<T: 'static, S: 'static, H: 'static, F: 'static>(
        mut self,
        queue: Queue<T, S, H>,
    ) -> Self
    where
        S: Storage<Output = T> + Unpin,
        T: Serialize + Debug + DeserializeOwned + Send,
        H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = F>
            + Unpin
            + Send
            + 'static,
        F: Future,
    {
        let addr = queue.start();
        self.addrs.push(addr.into());
        self
    }

    /// Register multiple queues that run on a separate thread.
    pub fn register_with_count<F, T, S, H, Fut>(mut self, count: usize, factory: F) -> Self
    where
        F: Fn() -> Addr<Queue<T, S, H>>,
        S: Storage<Output = T> + Unpin + Send + 'static,
        T: Serialize + Debug + DeserializeOwned + Send + 'static,
        H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = Fut>
            + Unpin
            + Send
            + 'static,
        Fut: Future + 'static,
    {
        for _worker in 0..count {
            let addr = factory();
            self.addrs.push(addr.into());
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestConsumer;
    impl Consumer for TestConsumer {}

    impl Actor for TestConsumer {
        type Context = Context<Self>;
    }

    #[actix_rt::test]
    async fn test_worker() {
        let res = Worker::new().register_with_threads(2, move || TestConsumer);
        assert!(Some(res).is_some())
    }
}
