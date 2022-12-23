use super::{Actor, Context, Handler, Message, Recipient, Worker};
use std::fmt::Debug;
use tokio::task::JoinHandle;

use std::fmt;

#[cfg(feature = "broker")]
use crate::worker::broker::Broker;

/// Represents a [Monitor] management message.
///
/// This is mainly sent by [Monitor] to [Worker] to:
///     - Check QueueWorker Status via [QueueStatus]
///     - Restart, stop and manage [Worker]
///     - Force acknowledge or kill jobs in a [Worker]
#[derive(Debug)]
pub enum WorkerManagement {
    Status,
    Restart,
    /// Kill specific job through [SpawnHandle]
    // KillJob(String),
    // (StorageWorkerConfig),
    Terminate,
}

impl<T> Default for Monitor<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a monitor for multiple instances of [Recipient] to [Worker].
///
///
/// Keeps an address of each queue and periodically checks of their status
/// When combined with the `web` feature, it can be used to manage the queues from a web ui.
pub struct Monitor<R> {
    workers: Vec<R>,
    event_handlers: Vec<Box<dyn WorkerListener>>,
}

impl<R> fmt::Debug for Monitor<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Monitor")
            .field("workers", &self.workers.len())
            .field("listeners", &self.event_handlers.len())
            .finish()
    }
}

#[async_trait::async_trait]
impl Actor for Monitor<Recipient<WorkerManagement>> {
    async fn on_start(&mut self, _ctx: &mut Context<Self>) {
        #[cfg(feature = "broker")]
        Broker::global().subscribe::<WorkerMessage, _>(_ctx);
        // loop {
        //     for worker in &self.workers {
        //         let _res = worker.send(WorkerManagement::Status).await;
        //         tracing::trace!("Status: {:?}", _res);
        //     }
        //     tokio::time::sleep(Duration::from_secs(1)).await;
        // }
    }
}

impl<T> Monitor<T> {
    /// Build  a new [Monitor] instance
    pub fn new() -> Self {
        Self {
            workers: Vec::new(),
            event_handlers: Vec::new(),
        }
    }
}

impl Monitor<JoinHandle<Recipient<WorkerManagement>>> {
    /// Register single worker instance of [Worker]
    pub fn register<W>(mut self, worker: W) -> Self
    where
        W: Worker,
    {
        let addr = tokio::spawn(async {
            let addr = worker.start().await;
            addr.recipient()
        });
        self.workers.push(addr);
        self
    }

    /// Register multiple worker instances of [Worker]
    pub fn register_with_count<F, W: Worker>(self, count: usize, factory: F) -> Self
    where
        F: Fn(usize) -> W,
    {
        let mut this = self;
        for index in 0..count {
            this = this.register(factory(index));
        }
        this
    }

    /// Start monitor without listening for Ctrl + C
    /// TODO: add the signals feature
    pub async fn run_without_signals(self) -> anyhow::Result<()> {
        let mut workers = Vec::new();
        for worker in self.workers {
            workers.push(worker.await?);
        }
        let monitor = Monitor {
            workers,
            event_handlers: self.event_handlers,
        };
        monitor.start().await;
        Ok(())
    }

    /// Start monitor listening for Ctrl + C
    pub async fn run(self) -> anyhow::Result<()> {
        self.run_without_signals().await?;
        log::debug!("Listening shut down command (ctrl + c)");
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for event");

        log::debug!("Workers shutdown complete");
        Ok(())
    }
}

/// Represents behaviour for listening to workers events via [Monitor]
pub trait WorkerListener: Send {
    ///  Called when an event is thrown by a worker
    fn on_event(&self, worker_id: &str, event: &WorkerEvent);
}

impl<K> Monitor<K> {
    /// Attach a new [WorkerListener] instance to [Monitor]
    #[cfg(feature = "broker")]
    pub fn event_handler<H: 'static>(mut self, handle: H) -> Self
    where
        H: WorkerListener,
    {
        self.event_handlers.push(Box::new(handle));
        self
    }
}

/// Represents events produced from a [Worker] Instance
#[derive(Debug, Clone)]
pub enum WorkerEvent {
    /// Emitted when a worker encounters a problem outside a job's processing scope
    /// Error(WorkerError),
    Error(String),
    /// Emitted when a job is processed
    Job {
        /// The job id
        id: String,
        // The job result
        // result: Result<JobResult, JobError>,
    },
}

/// Represents a message emitted by worker
#[derive(Debug, Clone)]
pub struct WorkerMessage {
    /// The event
    event: WorkerEvent,
    /// The worker id
    worker_id: String,
}

impl WorkerMessage {
    /// Generate a new [WorkerMessage].
    pub fn new(worker_id: String, event: WorkerEvent) -> Self {
        WorkerMessage { event, worker_id }
    }
}

impl Message for WorkerMessage {
    type Result = ();
}

#[async_trait::async_trait]
impl Handler<WorkerMessage> for Monitor<Recipient<WorkerManagement>> {
    type Result = ();

    async fn handle(&mut self, msg: WorkerMessage) -> Self::Result {
        for event_handler in &self.event_handlers {
            (event_handler).on_event(&msg.worker_id, &msg.event);
        }
    }
}

#[cfg(test)]
mod tests {

    use futures::Future;
    use tower::Service;

    use crate::{
        context::JobContext,
        error::JobError,
        job::{Job, JobStreamResult},
        job_fn::job_fn,
        request::JobRequest,
        response::JobResult,
    };

    use super::*;

    #[tokio::test]
    async fn test_simple_worker() {
        struct SimpleWorker<S>(S);

        #[derive(Debug, serde::Serialize, serde::Deserialize)]
        struct Email;

        impl Job for Email {
            const NAME: &'static str = "worker::Email";
        }

        async fn send_email(_job: Email, _ctx: JobContext) -> Result<JobResult, JobError> {
            Ok(JobResult::Success)
        }

        impl<S, F> Worker for SimpleWorker<S>
        where
            S: 'static
                + Send
                + Service<JobRequest<Email>, Response = JobResult, Error = JobError, Future = F>,
            F: Future<Output = Result<JobResult, JobError>> + Send + 'static,
        {
            type Job = Email;
            type Service = S;
            type Future = F;

            fn service(&mut self) -> &mut S {
                &mut self.0
            }

            fn consume(&mut self) -> JobStreamResult<Self::Job> {
                use futures::stream;
                let stream = stream::iter(vec![
                    Ok(Some(JobRequest::new(Email))),
                    Ok(Some(JobRequest::new(Email))),
                    Ok(Some(JobRequest::new(Email))),
                ]);
                Box::pin(stream)
            }
        }
        let res = Monitor::new()
            .register_with_count(1, move |_| SimpleWorker(job_fn(send_email)))
            .run_without_signals()
            .await;
        assert!(res.is_ok())
    }
}
