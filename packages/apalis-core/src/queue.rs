use std::fmt::Debug;

use actix::{Actor, ActorContext, Addr, AsyncContext, Context, Handler, StreamHandler};
use anyhow::Context as AnyHowContext;
use futures::{Future, StreamExt};
use serde::{de::DeserializeOwned, Serialize};
use tower::{Service, ServiceExt};

use crate::{
    error::{JobError, QueueError, StorageError},
    request::JobRequest,
    response::JobResult,
    storage::{self, Storage},
    streams::FetchJob,
    worker::WorkerManagement,
    worker::{QueueEvent, Worker},
};
/// A queue represents a consumer of a [Storage].
///
/// A [Service] must be provided to be called when a new job is detected.
#[must_use]
pub struct Queue<T: Serialize, S: Storage<Output = T>, H> {
    storage: S,
    status: QueueStatus,
    handler: Box<H>,
    monitor: Option<Addr<Worker>>,
}

/// Each [Queue] sends heartbeat messages
#[derive(Debug, Clone, Hash, PartialEq, Eq)]

pub enum Heartbeat {
    EnqueueScheduled(i32),
    RenqueueActive,
    Register,
    RenqueueOrpharned,
    Other(&'static str),
}

/// Represents the status of a queue.
///
/// Mainly consumed by [Worker]
#[derive(Default, Clone)]
pub struct QueueStatus {
    load: i64,
    //since: chrono::DateTime<chrono::Local>,
    id: uuid::Uuid,
}

struct JobRequestWrapper<T>(Result<Option<JobRequest<T>>, StorageError>);

impl<T, S, H> Queue<T, S, H>
where
    T: 'static + Serialize + Debug + DeserializeOwned,
    S: 'static + Storage<Output = T> + Unpin,
{
    pub fn new(storage: S, handler: H) -> Self {
        Queue {
            storage,
            handler: Box::from(handler),
            status: Default::default(),
            monitor: None,
        }
    }
}

impl<T: 'static, S: 'static, H: 'static, F: 'static> Actor for Queue<T, S, H>
where
    S: Storage + Unpin + Storage<Output = T>,
    T: Serialize + Debug + DeserializeOwned + Send,
    H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = F>
        + Unpin
        + Send
        + 'static,
    F: Future<Output = Result<JobResult, JobError>>,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.notify(WorkerManagement::Setup);
        let stream = self.storage.consume();
        let stream = stream.map(|c| JobRequestWrapper(c));
        ctx.add_stream(stream);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {}
}

impl<T: 'static, S: 'static, H: 'static, F: 'static> Handler<WorkerManagement> for Queue<T, S, H>
where
    S: Storage + Unpin + Storage<Output = T>,
    T: Serialize + Debug + DeserializeOwned + Send,
    H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = F>
        + Unpin
        + Send
        + 'static,
    F: Future<Output = Result<JobResult, JobError>>,
{
    type Result = Result<QueueStatus, QueueError>;

    fn handle(&mut self, msg: WorkerManagement, ctx: &mut Self::Context) -> Self::Result {
        let Queue { status, .. } = self;
        let res = match msg {
            WorkerManagement::Status => {}
            WorkerManagement::Stop => ctx.stop(),
            WorkerManagement::Restart => {}
            WorkerManagement::Setup => {
                let res = self.storage.heartbeat(Heartbeat::Register);
                let fut = async move {
                    res.await.unwrap();
                };
                let fut = actix::fut::wrap_future::<_, Self>(fut);
                ctx.spawn(fut);
            }
            WorkerManagement::Ack(_) => todo!(),
            WorkerManagement::Kill(_) => todo!(),
            WorkerManagement::Monitor(addr) => self.monitor = Some(addr),
        };
        Ok(status.clone())
    }
}

impl<T: 'static, S: 'static, H: 'static, F: 'static> StreamHandler<Heartbeat> for Queue<T, S, H>
where
    S: Storage<Output = T> + Unpin,
    T: Serialize + Debug + DeserializeOwned + Send,
    H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = F>
        + Unpin
        + Send
        + 'static,
    F: Future<Output = Result<JobResult, JobError>>,
{
    fn handle(&mut self, beat: Heartbeat, ctx: &mut Self::Context) {
        let queue = &mut self.storage;
        let heartbeat = queue.heartbeat(beat);
        let fut = async {
            heartbeat.await.unwrap();
        };
        let fut = actix::fut::wrap_future::<_, Self>(fut);
        ctx.spawn(fut);
    }
}

impl<T: 'static, S: 'static, H: 'static, F: 'static> StreamHandler<JobRequestWrapper<T>>
    for Queue<T, S, H>
where
    S: Storage<Output = T> + Unpin,
    T: Serialize + Debug + DeserializeOwned + Send,
    H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = F>
        + Unpin
        + Send
        + 'static,
    F: Future<Output = Result<JobResult, JobError>>,
{
    fn handle(&mut self, job: JobRequestWrapper<T>, ctx: &mut Self::Context) {
        let mut storage = self.storage.clone();
        let monitor = self.monitor.clone();
        match job.0 {
            Ok(Some(job)) => {
                let fut = {
                    let service: *mut Box<H> = &mut self.handler;
                    async move {
                        let handle = unsafe {
                            let handle = (*service).ready().await.unwrap();
                            handle
                        };
                        let id = job.id();

                        let res = handle.call(job).await.with_context(|| {
                            format!(
                                "Job [{}] Failed to complete job in queue {}",
                                id,
                                std::any::type_name::<T>()
                            )
                        });
                        let job_id = id.clone();
                        let addr = monitor.clone();
                        let finalize = match res {
                            Ok(r) => {
                                if let Some(addr) = monitor {
                                    addr.do_send(QueueEvent::Complete(id.clone(), r.clone()))
                                }
                                match r {
                                    JobResult::Success => storage.ack(id).await,
                                    JobResult::Retry => storage.retry(id).await,
                                    JobResult::Kill => storage.kill(id).await,
                                    JobResult::Reschedule(wait) => {
                                        storage.reschedule(id, wait).await
                                    }
                                }
                            }
                            Err(e) => {
                                if let Some(addr) = monitor {
                                    addr.do_send(QueueEvent::Failed(
                                        id.clone(),
                                        JobError::Failed(Box::from(e)),
                                    ))
                                }
                                storage.reschedule(id, chrono::Duration::seconds(1)).await
                            }
                        };
                        if let Err(e) = finalize {
                            if let Some(addr) = addr {
                                addr.do_send(QueueEvent::Failed(
                                    job_id,
                                    JobError::Storage(StorageError::Database(Box::from(e))),
                                ));
                            }
                        }
                    }
                };

                let fut = actix::fut::wrap_future::<_, Self>(fut);
                ctx.spawn(fut);
            }
            Ok(None) => {
                // println!("None")
            }
            Err(e) => {
                let addr = monitor.clone();
                if let Some(addr) = addr {
                    addr.do_send(QueueEvent::Error(QueueError::Storage(e)));
                }
            }
        }
    }
    fn finished(&mut self, ctx: &mut Self::Context) {
        // Restart streaming if met an error
        log::debug!("Encountered a storage error, restarting Streaming jobs");
        let stream = self.storage.consume();
        let stream = stream.map(|c| JobRequestWrapper(c));
        ctx.add_stream(stream);
    }
}
