use std::fmt::Debug;

use actix::prelude::*;
use futures::Future;
use serde::{de::DeserializeOwned, Serialize};
use tower::Service;

use crate::{
    error::JobError,
    error::QueueError,
    queue::{Queue, QueueStatus},
    request::JobRequest,
    response::JobResult,
    storage::Storage,
};

/// Represents a [Worker] management message.
///
/// This is mainly sent by [Worker] to [Queue] to:
///     - Check QueueWorker Status via [QueueStatus]
///     - Restart, stop and manage [Queue]
///     - Force acknowledge or kill jobs in a [Queue]
#[derive(Message)]
#[rtype(result = "Result<QueueStatus, QueueError>")]
pub enum WorkerManagement {
    Status,
    Stop,
    Restart,
    Setup,
    Ack(String),
    Kill(String),
    Monitor(Addr<Worker>),
}

impl Worker {
    pub fn new() -> Self {
        Self { addrs: Vec::new() }
    }
    pub async fn run(self) -> std::io::Result<()> {
        let queues = self.addrs.clone();
        let addr = self.start();
        for queue in queues {
            queue
                .send(WorkerManagement::Monitor(addr.clone()))
                .await
                .unwrap();
        }
        actix_rt::signal::ctrl_c().await
    }
}

/// Represents a monitor for multiple instances of [Recipient] to [Queue].
///
///
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

impl Actor for Worker {
    type Context = Context<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        log::info!(
            "Worker started with {} queues instances running.",
            self.addrs.len()
        )
    }
}

/// Represents events produced from a [Queue] Instance
///
#[derive(Debug, Message)]
#[rtype(result = "()")]
pub enum QueueEvent {
    Error(QueueError),
    Complete(String, JobResult),
    Failed(String, JobError),
}

impl Handler<QueueEvent> for Worker {
    type Result = ();
    fn handle(&mut self, msg: QueueEvent, ctx: &mut Self::Context) -> Self::Result {
        log::debug!("Received an event from Queue: {:?}", msg);
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
