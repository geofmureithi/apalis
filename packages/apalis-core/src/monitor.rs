use std::{fmt::Debug, time::Duration};

use actix::prelude::*;
use futures::Future;
use serde::{de::DeserializeOwned, Serialize};
use tower::Service;

use crate::{
    error::JobError,
    error::WorkerError,
    job::Job,
    request::JobRequest,
    response::JobResult,
    storage::Storage,
    worker::{Worker, WorkerConfig, WorkerStatus},
};

/// Represents a [Monitor] management message.
///
/// This is mainly sent by [Worker] to [Worker] to:
///     - Check QueueWorker Status via [QueueStatus]
///     - Restart, stop and manage [Worker]
///     - Force acknowledge or kill jobs in a [Worker]
#[derive(Message, Debug)]
#[rtype(result = "Result<WorkerStatus, WorkerError>")]
pub enum WorkerManagement {
    Status,
    Restart,
    Monitor(Addr<Monitor>),
    /// Kill specific job through [SpawnHandle]
    KillJob(String),
    Config(WorkerConfig),
    Terminate,
}

impl Monitor {
    pub fn new() -> Self {
        Self {
            addrs: Vec::new(),
            event_handlers: Vec::new(),
        }
    }

    pub async fn run(self) -> std::io::Result<()> {
        self.run_without_signals().await;
        actix_rt::signal::ctrl_c().await
    }

    pub async fn run_without_signals(self) {
        let queues = self.addrs.clone();
        let addr = self.start();
        for queue in queues {
            let res = queue.send(WorkerManagement::Monitor(addr.clone())).await;
            match res {
                Ok(Ok(status)) => {
                    tracing::debug!(
                        consumer_id = ?status.id,
                        load = status.load,
                        "worker.ready"
                    );
                }
                _ => tracing::warn!(
                    consumer_id = "unknown",
                    with_error = "worker may be unresponsive",
                    "worker.disappeared"
                ),
            };
        }
    }
}

/// Represents a monitor for multiple instances of [Recipient] to [Worker].
///
///
/// Keeps an address of each queue and periodically checks of their status
/// When combined with the `web` feature, it can be used to manage the queues from a web ui.
pub struct Monitor {
    addrs: Vec<Recipient<WorkerManagement>>,
    event_handlers: Vec<Box<dyn WorkerListener>>,
}

pub trait WorkerListener {
    fn on_event(&self, ctx: &mut Context<Monitor>, worker_id: &String, event: &WorkerEvent);
    fn subscribe(&self, _ctx: &mut Context<Monitor>) {
        //You may want to setup listening to other workers.
    }
}

impl Monitor {
    /// Register a single queue
    pub fn register<T: 'static, S: 'static, H: 'static, F: 'static, C>(
        mut self,
        queue: Worker<T, S, H>,
    ) -> Self
    where
        S: Storage<Output = T> + Unpin,
        T: Job + Serialize + Debug + DeserializeOwned + Send,
        H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = F>
            + Unpin
            + Send
            + 'static,
        F: Future<Output = Result<JobResult, JobError>>,
    {
        let addr = Supervisor::start(|_| queue);
        self.addrs.push(addr.into());
        self
    }

    /// Register multiple queues that run on a separate thread.
    pub fn register_with_count<F, T, S, H, Fut>(mut self, count: usize, factory: F) -> Self
    where
        F: Fn(usize) -> Addr<Worker<T, S, H>>,
        S: Storage<Output = T> + Unpin + Send + 'static,
        T: Job + Serialize + Debug + DeserializeOwned + Send + 'static,
        H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = Fut>
            + Unpin
            + Send
            + 'static,
        Fut: Future<Output = Result<JobResult, JobError>> + 'static,
    {
        for index in 0..count {
            let addr = factory(index);
            self.addrs.push(addr.into());
        }
        self
    }

    pub fn event_handler<H: 'static>(mut self, handle: H) -> Self
    where
        H: WorkerListener,
    {
        self.event_handlers.push(Box::new(handle));
        self
    }
}

impl Actor for Monitor {
    type Context = Context<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        log::debug!(
            "Monitor started with {} worker instances running.",
            self.addrs.len()
        );

        for event_handler in &self.event_handlers {
            event_handler.subscribe(ctx);
        }

        ctx.run_interval(Duration::from_secs(10), |act, ctx| {
            let queues = act.addrs.clone();
            let fut = async {
                for queue in queues {
                    let res = queue.send(WorkerManagement::Status).await;
                    match res {
                        Ok(Ok(status)) => {
                            tracing::trace!(
                                consumer_id = ?status.id,
                                load = status.load,
                                "queue.heartbeat"
                            );
                        }
                        _ => tracing::warn!(
                            consumer_id = "unknown",
                            with_error = "queue may be unresponsive",
                            "queue.heartbeat"
                        ),
                    };
                }
            };
            let fut = actix::fut::wrap_future::<_, Self>(fut);
            ctx.spawn(fut);
        });
    }
}

/// Represents events produced from a [Worker] Instance
///
#[derive(Debug)]
pub enum WorkerEvent {
    Error(WorkerError),
    Complete(String, JobResult),
    Failed(String, JobError),
}

impl WorkerEvent {
    pub(crate) fn with_worker(self, worker_id: String) -> WorkerMessage {
        WorkerMessage {
            event: self,
            worker_id,
        }
    }
}

#[derive(Debug, Message)]
#[rtype(result = "()")]
pub struct WorkerMessage {
    event: WorkerEvent,
    worker_id: String,
}

impl Handler<WorkerMessage> for Monitor {
    type Result = ();
    fn handle(&mut self, msg: WorkerMessage, ctx: &mut Self::Context) -> Self::Result {
        for event_handler in &self.event_handlers {
            (event_handler).on_event(ctx, &msg.worker_id, &msg.event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[actix_rt::test]
    async fn test_worker() {
        let res = Monitor::new().register_with_count(2, move || TestConsumer);
        assert!(Some(res).is_some())
    }
}
