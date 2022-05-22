use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    time::Duration,
};

use actix::{
    clock::{interval_at, Instant},
    ActorFutureExt,
};
use actix::{
    fut::wrap_future, Actor, ActorContext, Addr, AsyncContext, Context, Handler, SpawnHandle,
    StreamHandler, Supervised,
};
use chrono::Utc;
use futures::{Future, StreamExt};
use serde::{de::DeserializeOwned, Serialize};
use tower::{Service, ServiceExt};
use tracing::{Level, Span};

use crate::{
    context::JobTracker,
    error::{JobError, StorageError, WorkerError},
    job::Job,
    monitor::WorkerManagement,
    monitor::{Monitor, WorkerEvent},
    request::{
        JobReport, JobRequest,
        JobState::{self, Pending, Running},
        Report::Progress,
    },
    response::JobResult,
    storage::{self, Storage},
    streams::{FetchJob, HeartbeatStream},
};
use tracing_futures::Instrument;
/// A queue represents a consumer of a [Storage].
///
/// A [Service] must be provided to be called when a new job is detected.
#[must_use]
pub struct Worker<T: Serialize, S: Storage<Output = T>, H, C> {
    storage: S,
    handler: Box<H>,
    monitor: Option<Addr<Monitor>>,
    controller: C,
    jobs: BTreeMap<String, JobHandle>,
    id: uuid::Uuid,
    root_span: Span,
}

/// Each [Queue] sends heartbeat messages
#[non_exhaustive]
#[derive(Debug, Clone, Hash, PartialEq, Eq)]

pub enum WorkerPulse {
    EnqueueScheduled { count: i32 },
    RenqueueOrpharned { count: i32 },
}

pub trait OnCleanup {
    fn on_clean_up(
        &self,
        // The response from job
        response: &JobResult,
        // The result of the cleanup
        result: &Result<(), StorageError>,
    );
}

impl OnCleanup for () {
    #[inline]
    fn on_clean_up(&self, _: &JobResult, _: &Result<(), StorageError>) {}
}

impl<F> OnCleanup for F
where
    F: Fn(&JobResult, &Result<(), StorageError>),
{
    fn on_clean_up(&self, response: &JobResult, result: &Result<(), StorageError>) {
        self(response, result)
    }
}

#[derive(Clone)]
pub struct DefaultController;

impl<T: Job> WorkerController<T> for DefaultController {}

pub trait WorkerController<J>
where
    Self: Clone,
    J: Job,
{
    fn make_job_span(&self, worker_id: String, job: &JobRequest<J>) -> Span {
        let consumer_id: String = worker_id.to_string().chars().take(8).collect();
        let worker_span = tracing::span!(
            Level::INFO,
            "worker",
            job_type = format_args!("{}", J::NAME),
            id = format_args!("{}..", consumer_id),
        );
        let job_span = tracing::span!(
            parent: &worker_span,
            Level::INFO,
            "job",
            job_id = format_args!("{}", job.id()),
        );
        job_span
    }

    fn on_clean_up(&self, result: &Result<(), StorageError>) {
        tracing::info!("process.cleanup");
    }

    fn on_storage_error(&self, error: &StorageError) {
        tracing::info!("storage.error");
    }

    fn on_service_ready(&self, latency: Duration) {
        tracing::info!(latency = ?latency, "service.ready");
    }

    fn fetch_interval(&self) -> Duration {
        Duration::from_millis(50)
    }

    fn keep_alive(&self) -> Duration {
        Duration::from_secs(30)
    }

    fn heartbeats(&self) -> HashMap<WorkerPulse, Duration> {
        let mut pulse_map = HashMap::new();
        pulse_map.insert(
            WorkerPulse::RenqueueOrpharned { count: 10 },
            Duration::from_secs(60),
        );
        pulse_map.insert(
            WorkerPulse::EnqueueScheduled { count: 10 },
            Duration::from_secs(60),
        );
        pulse_map
    }
}

/// Represents the status of a queue.
///
/// Mainly consumed by [Worker]
#[derive(Default, Clone)]
pub struct WorkerStatus {
    pub(crate) load: usize,
    pub(crate) id: uuid::Uuid,
}

struct JobRequestWrapper<T>(Result<Option<JobRequest<T>>, StorageError>);

struct JobHandle {
    fut: SpawnHandle,
    progress: u8,
}

impl<T, S, H> Worker<T, S, H, DefaultController>
where
    T: 'static + Job + Serialize + Debug + DeserializeOwned,
    S: 'static + Storage<Output = T> + Unpin,
{
    pub fn new(storage: S, handler: H) -> Self {
        let id = uuid::Uuid::new_v4();
        let consumer_id: String = id.to_string().chars().take(8).collect();
        let span = tracing::span!(
            Level::INFO,
            "queue",
            job_type = T::NAME,
            consumer_id = format_args!("{}", consumer_id),
        );
        Worker {
            storage,
            handler: Box::from(handler),
            monitor: None,
            controller: DefaultController,
            jobs: BTreeMap::new(),
            id,
            root_span: span,
        }
    }
    pub fn controller<C>(mut self, controller: C) -> Worker<T, S, H, C> {
        Worker {
            storage: self.storage,
            handler: self.handler,
            monitor: self.monitor,
            controller,
            jobs: self.jobs,
            id: self.id,
            root_span: self.root_span,
        }
    }
}

impl<T: 'static, S: 'static, H: 'static, F: 'static, C> Actor for Worker<T, S, H, C>
where
    S: Storage + Unpin + Storage<Output = T>,
    T: Job + Serialize + Debug + DeserializeOwned + Send,
    H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = F>
        + Unpin
        + Send
        + 'static,
    F: Future<Output = Result<JobResult, JobError>>,
    C: WorkerController<T> + Unpin + Send + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // First lets do the first ping
        let res = self.storage.keep_alive(self.id.to_string());
        let fut = async move {
            let _res = res.await.unwrap();
        };
        let fut = wrap_future::<_, Self>(fut);
        ctx.spawn(fut);

        // Lets setup a ping based on controller keep_alive
        // To change this just modify the controller then restart.
        ctx.run_interval(self.controller.keep_alive(), |act, ctx| {
            let id = act.id.to_string();
            let storage = &mut act.storage;
            let res = storage.keep_alive(id);
            let fut = async move {
                let _res = res.await;
            };
            let fut = wrap_future::<_, Self>(fut);
            ctx.spawn(fut);
        });
        // Sets up reactivate orphaned jobs
        // Setup scheduling for non_sql storages eg Redis
        for (pulse, duration) in self.controller.heartbeats().into_iter() {
            let start = Instant::now() + Duration::from_millis(5);
            ctx.add_stream(HeartbeatStream::new(pulse, interval_at(start, duration)));
        }
        // Start Listening to incoming Jobs
        let stream = self
            .storage
            .consume(self.id.to_string(), self.controller.fetch_interval());
        let stream = stream.map(|c| JobRequestWrapper(c));

        ctx.add_stream(stream);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        tracing::warn!("worker.stopped")
    }
}

impl<T: 'static, S: 'static, H: 'static, F: 'static, C> Handler<JobReport> for Worker<T, S, H, C>
where
    S: Storage + Unpin + Storage<Output = T>,
    T: Job + Serialize + Debug + DeserializeOwned + Send,
    H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = F>
        + Unpin
        + Send
        + 'static,
    F: Future<Output = Result<JobResult, JobError>>,
    C: WorkerController<T> + Unpin + Send + 'static,
{
    type Result = Result<(), WorkerError>;

    fn handle(&mut self, msg: JobReport, _: &mut Self::Context) -> Self::Result {
        let span = &msg.span;
        span.in_scope(|| match &msg.report {
            Progress(progress) => {
                let handle = self.jobs.get_mut(&msg.job_id);
                match handle {
                    Some(handle) => handle.progress = *progress,
                    None => {
                        tracing::error!(
                            error = "trying to update a dropped job?",
                            "progress.update"
                        )
                    }
                };
            }
        });

        Ok(())
    }
}

impl<T: 'static, S: 'static, H: 'static, F: 'static, C> Handler<WorkerManagement>
    for Worker<T, S, H, C>
where
    S: Storage + Unpin + Storage<Output = T>,
    T: Job + Serialize + Debug + DeserializeOwned + Send,
    H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = F>
        + Unpin
        + Send
        + 'static,
    F: Future<Output = Result<JobResult, JobError>>,
    C: WorkerController<T> + Unpin + Send + 'static,
{
    type Result = Result<WorkerStatus, WorkerError>;

    fn handle(&mut self, msg: WorkerManagement, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            WorkerManagement::Status => {}
            WorkerManagement::Restart => ctx.stop(),
            WorkerManagement::Monitor(addr) => self.monitor = Some(addr),
            WorkerManagement::Kill(id) => {
                let mut storage = self.storage.clone();
                let worker_id = self.id.to_string();
                let job_id = id.clone();
                let fut = async move { storage.kill(worker_id, job_id).await }
                    .instrument(self.root_span.clone());
                let fut = wrap_future::<_, Self>(fut);
                let job_id = id.clone();
                let fut = fut.map(move |res, act, ctx| {
                    if res.is_ok() {
                        if let Some(handle) = act.jobs.remove(&job_id) {
                            ctx.cancel_future(handle.fut);
                        };
                    }
                });
                ctx.spawn(fut);
            }
        };
        Ok(WorkerStatus {
            load: self.jobs.len(),
            id: self.id.clone(),
        })
    }
}

impl<T: 'static, S: 'static, H: 'static, F: 'static, C> StreamHandler<WorkerPulse>
    for Worker<T, S, H, C>
where
    S: Storage<Output = T> + Unpin,
    T: Job + Serialize + Debug + DeserializeOwned + Send,
    H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = F>
        + Unpin
        + Send
        + 'static,
    F: Future<Output = Result<JobResult, JobError>>,
    C: WorkerController<T> + Unpin + Send + 'static,
{
    fn handle(&mut self, beat: WorkerPulse, ctx: &mut Self::Context) {
        let queue = &mut self.storage;
        let heartbeat = queue.heartbeat(beat);
        let fut = async {
            heartbeat.await;
        };
        let fut = wrap_future::<_, Self>(fut);
        ctx.spawn(fut);
    }
}

impl<T: 'static, S: 'static, H: 'static, F: 'static, C> StreamHandler<JobRequestWrapper<T>>
    for Worker<T, S, H, C>
where
    S: Storage<Output = T> + Unpin,
    T: Job + Serialize + Debug + DeserializeOwned + Send,
    H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = F>
        + Unpin
        + Send
        + 'static,
    F: Future<Output = Result<JobResult, JobError>>,
    C: WorkerController<T> + Unpin + Send + 'static,
{
    fn handle(&mut self, job: JobRequestWrapper<T>, ctx: &mut Self::Context) {
        let mut storage = self.storage.clone();
        let monitor = self.monitor.clone();

        //let span = self.root_span.clone();
        let job_tracker_addr = ctx.address().recipient();
        let controller = self.controller.clone();
        match job.0 {
            Ok(Some(mut job)) => {
                let job_id = job.id();
                let span = controller.make_job_span(self.id.to_string(), &job);

                let worker_id = self.id.to_string();
                let job_tracker = JobTracker::new(job_id.clone(), job_tracker_addr.clone());
                job.context_mut().set_tracker(job_tracker);
                job.set_status(JobState::Running);
                job.set_lock_at(Utc::now());
                job.record_attempt();
                job.set_lock_by(worker_id.clone());

                let job_id_ = job_id.clone();

                let fut = {
                    let service: *mut Box<H> = &mut self.handler;
                    async move {
                        let instant = Instant::now();
                        let handle = unsafe {
                            let handle = (*service).ready().await.unwrap();
                            handle
                        };
                        if let Err(e) = storage.update_by_id(job_id.clone(), &job).await {
                            if let Some(addr) = monitor.clone() {
                                controller.on_storage_error(&e);
                                addr.do_send(WorkerEvent::Error(WorkerError::Storage(e)));
                            }
                        };

                        controller.on_service_ready(instant.elapsed());
                        let res = handle.call(job).await;
                        let addr = monitor.clone();
                        if let Ok(Some(mut job)) = storage.fetch_by_id(job_id.clone()).await {
                            job.set_done_at(Utc::now());
                            let finalize = match res {
                                Ok(r) => {
                                    if let Some(addr) = monitor.clone() {
                                        addr.do_send(WorkerEvent::Complete(
                                            job_id.clone(),
                                            r.clone(),
                                        ))
                                    }
                                    match r {
                                        JobResult::Success => {
                                            job.set_status(JobState::Done);
                                            storage.ack(worker_id.clone(), job_id.clone()).await
                                        }
                                        JobResult::Retry => {
                                            job.set_status(JobState::Retry);
                                            storage.retry(worker_id.clone(), job_id.clone()).await
                                        }
                                        JobResult::Kill => {
                                            job.set_status(JobState::Killed);
                                            storage.kill(worker_id.clone(), job_id.clone()).await
                                        }

                                        JobResult::Reschedule(wait) => {
                                            job.set_status(JobState::Retry);
                                            storage.reschedule(job_id.clone(), wait).await
                                        }
                                    }
                                }
                                Err(e) => {
                                    job.set_status(JobState::Failed);
                                    job.set_last_error(format!("{}", e));
                                    if let Some(addr) = monitor.clone() {
                                        addr.do_send(WorkerEvent::Failed(job_id.clone(), e))
                                    }
                                    storage
                                        .reschedule(job_id.clone(), Duration::from_secs(5))
                                        .await
                                }
                            };
                            controller.on_clean_up(&finalize);

                            if let Err(e) = finalize {
                                if let Some(addr) = addr {
                                    addr.do_send(WorkerEvent::Failed(
                                        job_id.clone(),
                                        JobError::Storage(StorageError::Database(Box::from(e))),
                                    ));
                                }
                            }
                            if let Err(e) = storage.update_by_id(job_id.clone(), &job).await {
                                controller.on_storage_error(&e);
                                if let Some(addr) = monitor {
                                    addr.do_send(WorkerEvent::Error(WorkerError::Storage(e)));
                                }
                            };
                        } else {
                            panic!("Unable to update job");
                        }
                    }
                }
                .instrument(span);

                let fut = wrap_future::<_, Self>(fut);

                let remove_id = job_id_.clone();
                let fut = fut.map(move |res, act, ctx| {
                    act.jobs.remove_entry(&remove_id);
                });
                let handle = ctx.spawn(fut);
                self.jobs.insert(
                    job_id_,
                    JobHandle {
                        fut: handle,
                        progress: 0,
                    },
                );
            }
            Ok(None) => {
                // println!("None")
            }
            Err(e) => {
                tracing::warn!(error= ?e, "queue.stopping");
                let addr = monitor.clone();
                if let Some(addr) = addr {
                    addr.do_send(WorkerEvent::Error(WorkerError::Storage(e)));
                }
            }
        };
    }
}

impl<T: 'static, S: 'static, H: 'static, F: 'static, C> Supervised for Worker<T, S, H, C>
where
    S: Storage + Unpin + Storage<Output = T>,
    T: Job + Serialize + Debug + DeserializeOwned + Send,
    H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = F>
        + Unpin
        + Send
        + 'static,
    F: Future<Output = Result<JobResult, JobError>>,
    C: WorkerController<T> + Unpin + Send + 'static,
{
    fn restarting(&mut self, _: &mut <Self as Actor>::Context) {
        tracing::warn!("worker.restart");
    }
}
