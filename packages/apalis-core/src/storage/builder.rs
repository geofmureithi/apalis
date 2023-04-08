use crate::request::JobRequest;
use crate::worker::WorkerId;
use futures::future::BoxFuture;
use futures::FutureExt;
use futures::StreamExt;
use std::{marker::PhantomData, time::Duration};
use tower::layer::util::Stack;
use tower::Layer;
use tower::Service;

use crate::{builder::WorkerBuilder, job::JobStreamResult};

use super::beats::EnqueueScheduled;
use super::beats::ReenqueueOrphaned;
use super::{beats::KeepAlive, Storage};

/// A helper trait to help build a [Worker] that consumes a [Storage]
pub trait WithStorage<NS, ST: Storage<Output = Self::Job>>: Sized {
    /// The job to consume
    type Job;
    /// The [Stream] to produce jobs
    type Stream;
    /// The builder method to produce a default [WorkerBuilder] that will consume jobs
    fn with_storage(self, storage: ST) -> WorkerBuilder<Self::Job, Self::Stream, NS> {
        self.with_storage_config(storage, |e| e)
    }
    /// The builder method to produce a configured [WorkerBuilder] that will consume jobs
    fn with_storage_config(
        self,
        storage: ST,
        config: impl Fn(WorkerConfig) -> WorkerConfig,
    ) -> WorkerBuilder<Self::Job, Self::Stream, NS>;
}

/// Allows configuring of how storages are consumed
#[derive(Debug)]
pub struct WorkerConfig {
    keep_alive: Duration,
    enqueue_scheduled: Option<(i32, Duration)>,
    reenqueue_orphaned: Option<(i32, Duration)>,
    buffer_size: usize,
    fetch_interval: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            keep_alive: Duration::from_secs(30),
            enqueue_scheduled: Some((10, Duration::from_secs(10))),
            reenqueue_orphaned: Some((10, Duration::from_secs(10))),
            buffer_size: 1,
            fetch_interval: Duration::from_millis(50),
        }
    }
}

impl WorkerConfig {
    /// The number of jobs to fetch in one poll
    ///
    /// Defaults to 1
    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// The rate at which jobs in the scheduled queue are pushed into the active queue
    /// 
    /// Can be set to none for sql scenarios as sql uses run_at
    /// This mainly applies for redis currently
    pub fn enqueue_scheduled(mut self, interval: Option<(i32, Duration)>) -> Self {
        self.enqueue_scheduled = interval;
        self
    }

    /// The rate at which orphaned jobs are returned to the queue
    /// 
    /// If None then no garbage collection of orphaned jobs 
    pub fn reenqueue_orphaned(mut self, interval: Option<(i32, Duration)>) -> Self {
        self.reenqueue_orphaned = interval;
        self
    }

    /// The rate at which polling is occurring
    /// This may be ignored if the storage uses pubsub
    pub fn fetch_interval(mut self, interval: Duration) -> Self {
        self.fetch_interval = interval;
        self
    }
}

/// A layer that acknowledges a job completed successfully
#[derive(Debug)]
pub struct AckJobLayer<ST, J> {
    storage: ST,
    job_type: PhantomData<J>,
    worker_id: WorkerId,
}

impl<ST, J> AckJobLayer<ST, J> {
    /// Build a new [AckJobLayer] for a worker
    pub fn new(storage: ST, worker_id: WorkerId) -> Self {
        Self {
            storage,
            job_type: PhantomData,
            worker_id,
        }
    }
}

impl<ST, J, S> Layer<S> for AckJobLayer<ST, J>
where
    S: Service<JobRequest<J>> + Send + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
    S::Future: Send + 'static,
    ST: Storage<Output = J> + Send + Sync + 'static,
{
    type Service = AckJobService<S, ST, J>;

    fn layer(&self, service: S) -> Self::Service {
        AckJobService {
            service,
            storage: self.storage.clone(),
            job_type: PhantomData,
            worker_id: self.worker_id.clone(),
        }
    }
}

/// The underlying service for an [AckJobLayer]
#[derive(Debug)]
pub struct AckJobService<SV, ST, J> {
    service: SV,
    storage: ST,
    job_type: PhantomData<J>,
    worker_id: WorkerId,
}

impl<SV, ST, J> Service<JobRequest<J>> for AckJobService<SV, ST, J>
where
    SV: Service<JobRequest<J>> + Send + Sync + 'static,
    SV::Error: std::error::Error + Send + Sync + 'static,
    <SV as Service<JobRequest<J>>>::Future: std::marker::Send + 'static,
    ST: Storage<Output = J> + Send + 'static,
    <SV as Service<JobRequest<J>>>::Response: std::marker::Send,
{
    type Response = SV::Response;
    type Error = SV::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: JobRequest<J>) -> Self::Future {
        let mut storage = self.storage.clone();
        let job_id = request.id().clone();
        let worker_id = self.worker_id.clone();
        let fut = self.service.call(request);
        let fut_with_ack = async move {
            let res = fut.await;
            if res.is_ok() {
                storage.ack(&worker_id, &job_id).await.ok();
            }
            res
        };
        fut_with_ack.boxed()
    }
}

impl<J: 'static, M, ST> WithStorage<Stack<AckJobLayer<ST, J>, M>, ST> for WorkerBuilder<(), (), M>
where
    ST: Storage<Output = J> + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    type Job = J;
    type Stream = JobStreamResult<J>;
    fn with_storage_config(
        mut self,
        mut storage: ST,
        config: impl Fn(WorkerConfig) -> WorkerConfig,
    ) -> WorkerBuilder<J, Self::Stream, Stack<AckJobLayer<ST, J>, M>> {
        let worker_config = config(WorkerConfig::default());
        let worker_id = self.id;
        let source = storage
            .consume(
                &worker_id,
                worker_config.fetch_interval,
                worker_config.buffer_size,
            )
            .boxed();

        let layer = self.layer.layer(AckJobLayer {
            storage: storage.clone(),
            job_type: PhantomData,
            worker_id: worker_id.clone(),
        });

        let keep_alive: KeepAlive<ST, M> =
            KeepAlive::new::<J>(&worker_id, storage.clone(), worker_config.keep_alive);
        self.beats.push(Box::new(keep_alive));
        if let Some((count, duration)) = worker_config.reenqueue_orphaned {
            let reenqueue_orphaned = ReenqueueOrphaned::new(storage.clone(), count, duration);
            self.beats.push(Box::new(reenqueue_orphaned));
        }
        if let Some((count, duration)) = worker_config.enqueue_scheduled {
            let enqueue_scheduled = EnqueueScheduled::new(storage.clone(), count, duration);
            self.beats.push(Box::new(enqueue_scheduled));
        }
        WorkerBuilder {
            job: PhantomData,
            layer,
            source,
            id: worker_id,
            beats: self.beats,
        }
    }
}
