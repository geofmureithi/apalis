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
use tracing::Level;

use crate::{
    error::{JobError, StorageError, WorkerError},
    job::Job,
    monitor::WorkerManagement,
    monitor::{Monitor, WorkerEvent},
    request::{JobRequest, JobState},
    response::JobResult,
    storage::Storage,
    streams::HeartbeatStream,
};
use tracing_futures::Instrument;
/// A queue represents a consumer of a [Storage].
///
/// A [Service] must be provided to be called when a new job is detected.
#[must_use]
pub struct Worker<T: Serialize, S: Storage<Output = T>, H> {
    storage: S,
    handler: Box<H>,
    monitor: Option<Addr<Monitor>>,
    config: WorkerConfig,
    jobs: BTreeMap<String, JobHandle>,
    id: uuid::Uuid,
}

/// Each [Worker] sends heartbeat messages to storage
#[non_exhaustive]
#[derive(Debug, Clone, Hash, PartialEq, Eq)]

pub enum WorkerPulse {
    EnqueueScheduled { count: i32 },
    RenqueueOrpharned { count: i32 },
}

#[derive(Debug)]
pub struct WorkerConfig {
    keep_alive: Duration,
    fetch_interval: Duration,
    heartbeats: HashMap<WorkerPulse, Duration>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        let mut heartbeats = HashMap::new();
        heartbeats.insert(
            WorkerPulse::RenqueueOrpharned { count: 10 },
            Duration::from_secs(60),
        );
        heartbeats.insert(
            WorkerPulse::EnqueueScheduled { count: 10 },
            Duration::from_secs(60),
        );

        WorkerConfig {
            keep_alive: Duration::from_secs(30),
            fetch_interval: Duration::from_millis(50),
            heartbeats,
        }
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
}

impl<T, S, H> Worker<T, S, H>
where
    T: 'static + Job + Serialize + Debug + DeserializeOwned,
    S: 'static + Storage<Output = T> + Unpin,
{
    pub fn new(storage: S, handler: H) -> Self {
        let id = uuid::Uuid::new_v4();
        Worker {
            storage,
            handler: Box::from(handler),
            monitor: None,
            config: Default::default(),
            jobs: BTreeMap::new(),
            id,
        }
    }
    /// Set a [WorkerConfig] for [Worker]
    pub fn config(self, config: WorkerConfig) -> Worker<T, S, H> {
        Worker {
            storage: self.storage,
            handler: self.handler,
            monitor: self.monitor,
            config,
            jobs: self.jobs,
            id: self.id,
        }
    }
}

impl<T: 'static, S: 'static, H: 'static, F: 'static> Actor for Worker<T, S, H>
where
    S: Storage + Unpin + Storage<Output = T>,
    T: Job + Serialize + Debug + DeserializeOwned + Send,
    H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = F>
        + Unpin
        + Send
        + 'static,
    F: Future<Output = Result<JobResult, JobError>>,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // First lets do the first ping
        let res = self.storage.keep_alive(self.id.to_string());
        let fut = async move {
            let _res = res.await;
        };
        let fut = wrap_future::<_, Self>(fut);
        ctx.spawn(fut);

        // Lets setup a ping based on controller keep_alive
        // To change this just modify the controller then restart.
        ctx.run_interval(self.config.keep_alive, |act, ctx| {
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
        for (pulse, duration) in self.config.heartbeats.iter() {
            let start = Instant::now() + Duration::from_millis(5);
            ctx.add_stream(HeartbeatStream::new(
                pulse.clone(),
                interval_at(start, duration.clone()),
            ));
        }
        // Start Listening to incoming Jobs
        let stream = self
            .storage
            .consume(self.id.to_string(), self.config.fetch_interval.clone());
        let stream = stream.map(|c| JobRequestWrapper(c));

        ctx.add_stream(stream);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        tracing::warn!("worker.stopped")
    }
}

impl<T: 'static, S: 'static, H: 'static, F: 'static> Handler<WorkerManagement> for Worker<T, S, H>
where
    S: Storage + Unpin + Storage<Output = T>,
    T: Job + Serialize + Debug + DeserializeOwned + Send,
    H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = F>
        + Unpin
        + Send
        + 'static,
    F: Future<Output = Result<JobResult, JobError>>,
{
    type Result = Result<WorkerStatus, WorkerError>;

    fn handle(&mut self, msg: WorkerManagement, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            WorkerManagement::Status => {}
            WorkerManagement::Restart => ctx.stop(),
            WorkerManagement::Monitor(addr) => self.monitor = Some(addr),
            WorkerManagement::KillJob(id) => {
                let mut storage = self.storage.clone();
                let worker_id = self.id.to_string();
                let job_id = id.clone();
                let fut = async move { storage.kill(worker_id, job_id).await };
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
            WorkerManagement::Config(config) => {
                self.config = config;
                ctx.stop(); //Trigger a restart from supervised
            }
            WorkerManagement::Terminate => ctx.terminate(),
        };
        Ok(WorkerStatus {
            load: self.jobs.len(),
            id: self.id.clone(),
        })
    }
}

impl<T: 'static, S: 'static, H: 'static, F: 'static> StreamHandler<WorkerPulse> for Worker<T, S, H>
where
    S: Storage<Output = T> + Unpin,
    T: Job + Serialize + Debug + DeserializeOwned + Send,
    H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = F>
        + Unpin
        + Send
        + 'static,
    F: Future<Output = Result<JobResult, JobError>>,
{
    fn handle(&mut self, beat: WorkerPulse, ctx: &mut Self::Context) {
        let queue = &mut self.storage;
        let heartbeat = queue.heartbeat(beat);
        let fut = async {
            heartbeat.await.unwrap();
        };
        let fut = wrap_future::<_, Self>(fut);
        ctx.spawn(fut);
    }
}

impl<T: 'static, S: 'static, H: 'static, F: 'static> StreamHandler<JobRequestWrapper<T>>
    for Worker<T, S, H>
where
    S: Storage<Output = T> + Unpin,
    T: Job + Serialize + Debug + DeserializeOwned + Send,
    H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = F>
        + Unpin
        + Send
        + 'static,
    F: Future<Output = Result<JobResult, JobError>>,
{
    fn handle(&mut self, job: JobRequestWrapper<T>, ctx: &mut Self::Context) {
        let mut storage = self.storage.clone();
        let monitor = self.monitor.clone();
        let worker_id = self.id.to_string();
        match job.0 {
            Ok(Some(mut job)) => {
                let job_id = job.id();
                let remove_id = job_id.clone();

                #[cfg(feature = "trace")]
                #[cfg_attr(docsrs, doc(cfg(feature = "trace")))]
                let span = {
                    let consumer_id: String = worker_id.to_string().chars().take(8).collect();
                    let worker_span = tracing::span!(
                        Level::INFO,
                        "worker",
                        job_type = format_args!("{}", T::NAME),
                        id = format_args!("{}", consumer_id),
                    );
                    tracing::span!(
                        parent: &worker_span,
                        Level::INFO,
                        "job",
                        job_id = format_args!("{}", job.id()),
                    )
                };

                let fut = {
                    let service: *mut Box<H> = &mut self.handler;
                    async move {
                        let instant = Instant::now();
                        let handle = unsafe {
                            let handle = (*service).ready().await;
                            handle
                        };
                        match handle {
                            Ok(service) => {
                                job.set_status(JobState::Running);
                                job.set_lock_at(Some(Utc::now()));
                                job.record_attempt();
                                job.set_lock_by(Some(worker_id.clone()));
                                if let Err(e) = storage.update_by_id(job_id.clone(), &job).await {
                                    if let Some(addr) = monitor.clone() {
                                        T::on_storage_error(&job.inner(), &job, &e);
                                        addr.do_send(
                                            WorkerEvent::Error(WorkerError::Storage(e))
                                                .with_worker(worker_id.clone()),
                                        );
                                    }
                                };
                                T::on_service_ready(&job.inner(), &job, instant.elapsed());
                                let res = service.call(job).await;
                                let addr = monitor.clone();
                                if let Ok(Some(mut job)) = storage.fetch_by_id(job_id.clone()).await
                                {
                                    job.set_done_at(Some(Utc::now()));
                                    let finalize = match res {
                                        Ok(r) => {
                                            if let Some(addr) = monitor.clone() {
                                                addr.do_send(
                                                    WorkerEvent::Complete(
                                                        job_id.clone(),
                                                        r.clone(),
                                                    )
                                                    .with_worker(worker_id.clone()),
                                                )
                                            }
                                            match r {
                                                JobResult::Success => {
                                                    job.set_status(JobState::Done);
                                                    storage
                                                        .ack(worker_id.clone(), job_id.clone())
                                                        .await
                                                }
                                                JobResult::Retry => {
                                                    job.set_status(JobState::Retry);
                                                    storage
                                                        .retry(worker_id.clone(), job_id.clone())
                                                        .await
                                                }
                                                JobResult::Kill => {
                                                    job.set_status(JobState::Killed);
                                                    storage
                                                        .kill(worker_id.clone(), job_id.clone())
                                                        .await
                                                }

                                                JobResult::Reschedule(wait) => {
                                                    job.set_status(JobState::Retry);
                                                    storage.reschedule(&job, wait).await
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            job.set_status(JobState::Failed);
                                            job.set_last_error(format!("{}", e));
                                            if let Some(addr) = monitor.clone() {
                                                addr.do_send(
                                                    WorkerEvent::Failed(job_id.clone(), e)
                                                        .with_worker(worker_id.clone()),
                                                )
                                            }
                                            storage.reschedule(&job, Duration::from_secs(1)).await
                                        }
                                    };
                                    T::on_clean_up(&job.inner(), &job, &finalize);

                                    if let Err(e) = finalize {
                                        if let Some(addr) = addr {
                                            addr.do_send(
                                                WorkerEvent::Failed(
                                                    job_id.clone(),
                                                    JobError::Storage(StorageError::Database(
                                                        Box::from(e),
                                                    )),
                                                )
                                                .with_worker(worker_id.clone()),
                                            );
                                        }
                                    }
                                    if let Err(e) = storage.update_by_id(job_id.clone(), &job).await
                                    {
                                        T::on_storage_error(&job.inner(), &job, &e);
                                        if let Some(addr) = monitor {
                                            addr.do_send(
                                                WorkerEvent::Error(WorkerError::Storage(e))
                                                    .with_worker(worker_id.clone()),
                                            );
                                        }
                                    };
                                } else {
                                    panic!("Unable to update job");
                                }
                            }
                            Err(error) => {
                                let addr = monitor.clone();
                                if let Some(addr) = addr {
                                    addr.do_send(
                                        WorkerEvent::Failed(job_id.clone(), error)
                                            .with_worker(worker_id.clone()),
                                    );
                                }
                                job.set_status(JobState::Pending);
                                if let Err(e) = storage.update_by_id(job_id.clone(), &job).await {
                                    T::on_storage_error(&job.inner(), &job, &e);
                                    if let Some(addr) = monitor {
                                        addr.do_send(
                                            WorkerEvent::Error(WorkerError::Storage(e))
                                                .with_worker(worker_id.clone()),
                                        );
                                    }
                                };
                            }
                        }
                    }
                };

                #[cfg(feature = "trace")]
                let fut = fut.instrument(span);

                let fut = wrap_future::<_, Self>(fut);
                let job_id = remove_id.clone();
                let fut = fut.map(move |_res, act, _ctx| {
                    act.jobs.remove_entry(&remove_id);
                });
                let handle = ctx.spawn(fut);
                self.jobs.insert(job_id, JobHandle { fut: handle });
            }
            Ok(None) => {}
            Err(e) => {
                let addr = monitor.clone();
                if let Some(addr) = addr {
                    addr.do_send(
                        WorkerEvent::Error(WorkerError::Storage(e)).with_worker(worker_id.clone()),
                    );
                }
            }
        };
    }
}

impl<T: 'static, S: 'static, H: 'static, F: 'static> Supervised for Worker<T, S, H>
where
    S: Storage + Unpin + Storage<Output = T>,
    T: Job + Serialize + Debug + DeserializeOwned + Send,
    H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = F>
        + Unpin
        + Send
        + 'static,
    F: Future<Output = Result<JobResult, JobError>>,
{
    fn restarting(&mut self, _: &mut <Self as Actor>::Context) {
        tracing::warn!("worker.restart");
    }
}
