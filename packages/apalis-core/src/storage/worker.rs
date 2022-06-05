use std::{collections::HashMap, fmt::Debug, time::Duration};

use chrono::Utc;
use futures::Future;
use serde::{de::DeserializeOwned, Serialize};
use tokio::time::{interval, interval_at, Instant};
use tower::{Layer, Service, ServiceExt};

use crate::{
    builder::{WorkerBuilder, WorkerFactory},
    error::{JobError, WorkerError},
    job::{Job, JobStream},
    request::{JobRequest, JobState},
    response::JobResult,
    storage::{JobStreamResult, Storage},
    worker::{Context, Handler, Message, Worker},
};

#[cfg(feature = "broker")]
use crate::worker::{
    broker::Broker,
    prelude::{WorkerEvent, WorkerMessage},
};

use super::{
    streams::{HeartbeatStream, KeepAliveStream},
    StorageWorkerPulse,
};

/// Controls how [StorageWorker] interacts with the [Storage]
#[derive(Debug)]
pub struct StorageWorkerConfig {
    keep_alive: Duration,
    fetch_interval: Duration,
    heartbeats: HashMap<StorageWorkerPulse, Duration>,
}

impl Default for StorageWorkerConfig {
    fn default() -> Self {
        let mut heartbeats = HashMap::new();
        heartbeats.insert(
            StorageWorkerPulse::RenqueueOrpharned { count: 10 },
            Duration::from_secs(60),
        );
        heartbeats.insert(
            StorageWorkerPulse::EnqueueScheduled { count: 10 },
            Duration::from_secs(60),
        );

        StorageWorkerConfig {
            keep_alive: Duration::from_secs(30),
            fetch_interval: Duration::from_millis(50),
            heartbeats,
        }
    }
}

/// A queue represents a consumer of a [Storage].
///
/// A [Service] must be provided to be called when a new job is detected.
#[derive(Debug)]
pub struct StorageWorker<T: Serialize, S: Storage<Output = T>, H> {
    storage: S,
    handler: H,
    config: StorageWorkerConfig,
    id: uuid::Uuid,
}

impl<T, S, H> StorageWorker<T, S, H>
where
    T: 'static + Job + Serialize + Debug + DeserializeOwned,
    S: 'static + Storage<Output = T> + Unpin,
{
    /// Create a new Worker instance
    pub fn new(storage: S, handler: H) -> Self {
        let id = uuid::Uuid::new_v4();
        StorageWorker {
            storage,
            handler,
            config: Default::default(),
            id,
        }
    }
    /// Set a [WorkerConfig] for [Worker]
    pub fn config(self, config: StorageWorkerConfig) -> StorageWorker<T, S, H> {
        StorageWorker {
            storage: self.storage,
            handler: self.handler,
            config,
            id: self.id,
        }
    }
}

#[async_trait::async_trait]
impl<T, S, H, F> Worker for StorageWorker<T, S, H>
where
    S: JobStream<Job = T> + Storage + Unpin + Storage<Output = T> + Send + 'static + Sync,
    T: Job + Send + 'static,
    H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = F>
        + Unpin
        + Send
        + 'static,
    F: Future<Output = Result<JobResult, JobError>> + Send,
{
    type Job = T;

    type Service = H;

    type Future = F;

    async fn on_start(&mut self, ctx: &mut Context<Self>) {
        // To change this just modify the controller then restart.
        ctx.notify_with(KeepAliveStream::new(interval(self.config.keep_alive)));
        // Sets up reactivate orphaned jobs
        // Setup scheduling for non_sql storages eg Redis
        for (pulse, duration) in self.config.heartbeats.iter() {
            let start = Instant::now() + Duration::from_millis(17);
            ctx.notify_with(HeartbeatStream::new(
                pulse.clone(),
                interval_at(start, duration.clone()),
            ));
        }
    }

    async fn on_stop(&mut self, _ctx: &mut Context<Self>) {
        tracing::warn!("worker.stopped")
    }

    fn consume(&mut self) -> JobStreamResult<Self::Job> {
        self.storage
            .stream(self.id.to_string(), self.config.fetch_interval.clone())
    }

    fn service(&mut self) -> &mut Self::Service {
        &mut self.handler
    }

    async fn handle_job(&mut self, mut job: JobRequest<Self::Job>) -> Result<JobResult, JobError> {
        let instant = Instant::now();
        let mut storage = self.storage.clone();
        let worker_id = self.id.to_string();
        let handle = self.service().ready().await?;
        let job_id = job.id();
        job.set_status(JobState::Running);
        job.set_lock_at(Some(Utc::now()));
        job.record_attempt();
        job.set_lock_by(Some(worker_id.clone()));
        if let Err(e) = storage.update_by_id(job_id.clone(), &job).await {
            #[cfg(feature = "broker")]
            Broker::global()
                .issue_send(WorkerMessage::new(
                    worker_id.clone(),
                    WorkerEvent::Error(format!("{}", e)),
                ))
                .await;
            T::on_worker_error(&job.inner(), &job, &WorkerError::Storage(e));
        };
        T::on_service_ready(&job.inner(), &job, instant.elapsed());
        let res = handle.call(job).await;

        if let Ok(Some(mut job)) = storage.fetch_by_id(job_id.clone()).await {
            job.set_done_at(Some(Utc::now()));
            let finalize = match res {
                Ok(ref r) => match r {
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
                        storage.reschedule(&job, *wait).await
                    }
                },
                Err(ref e) => {
                    job.set_status(JobState::Failed);
                    job.set_last_error(format!("{}", e));

                    #[cfg(feature = "broker")]
                    Broker::global()
                        .issue_send(WorkerMessage::new(
                            worker_id.clone(),
                            WorkerEvent::Error(format!("{}", e)),
                        ))
                        .await;
                    // let base: i32 = 2; // an explicit type is required
                    // let millis = base.pow(job.attempts());
                    storage.reschedule(&job, Duration::from_millis(10000)).await
                }
            };
            if let Err(e) = finalize {
                #[cfg(feature = "broker")]
                Broker::global()
                    .issue_send(WorkerMessage::new(
                        worker_id.clone(),
                        WorkerEvent::Error(format!("{}", e)),
                    ))
                    .await;
                T::on_worker_error(&job.inner(), &job, &WorkerError::Storage(e));
            }
            if let Err(e) = storage.update_by_id(job_id.clone(), &job).await {
                #[cfg(feature = "broker")]
                Broker::global()
                    .issue_send(WorkerMessage::new(
                        worker_id.clone(),
                        WorkerEvent::Error(format!("{}", e)),
                    ))
                    .await;
                T::on_worker_error(&job.inner(), &job, &WorkerError::Storage(e));
            };
        }

        res
    }
}

impl Message for StorageWorkerPulse {
    type Result = ();
}

#[async_trait::async_trait]
impl<T: 'static, S: 'static, H: 'static, F> Handler<StorageWorkerPulse> for StorageWorker<T, S, H>
where
    S: Storage<Output = T> + Unpin + Send + Sync,
    T: Job + Serialize + Debug + DeserializeOwned + Send,
    H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = F>
        + Unpin
        + Send
        + 'static,
    F: Future<Output = Result<JobResult, JobError>> + Send,
{
    type Result = ();
    async fn handle(&mut self, beat: StorageWorkerPulse) -> Self::Result {
        let queue = &mut self.storage;
        let _heartbeat = queue.heartbeat(beat).await;
    }
}

pub(crate) struct KeepAlive;

impl Message for KeepAlive {
    type Result = ();
}

#[async_trait::async_trait]
impl<T: 'static, S: 'static, H: 'static, F> Handler<KeepAlive> for StorageWorker<T, S, H>
where
    S: Storage<Output = T> + Unpin + Send + Sync,
    T: Job + Serialize + Debug + DeserializeOwned + Send,
    H: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = F>
        + Unpin
        + Send
        + 'static,
    F: Future<Output = Result<JobResult, JobError>> + Send,
{
    type Result = ();
    async fn handle(&mut self, _keep_alive: KeepAlive) -> Self::Result {
        let queue = &mut self.storage;
        let id = self.id.to_string();
        let _beat = queue.keep_alive::<H>(id).await;
    }
}

impl<T, S, M, Ser, Fut> WorkerFactory<Ser> for WorkerBuilder<T, S, M>
where
    S: Storage<Output = T> + Unpin + Send + 'static + Sync,
    T: Job + Serialize + Debug + DeserializeOwned + Send + 'static,
    M: Layer<Ser>,
    <M as Layer<Ser>>::Service: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = Fut>
        + Unpin
        + Send
        + 'static,
    Fut: Future<Output = Result<JobResult, JobError>> + Send,
{
    type Worker = StorageWorker<T, S, <M as Layer<Ser>>::Service>;

    fn build(self, service: Ser) -> Self::Worker {
        StorageWorker::new(self.source, self.layer.layer(service))
    }
}
