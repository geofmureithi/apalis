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

use super::{beats::KeepAlive, Storage};

/// A helper trait to help build a [Worker] that consumes a [Storage]
pub trait WithStorage<NS, ST: Storage<Output = Self::Job>> {
    /// The job to consume
    type Job;
    /// The [Stream] to produce jobs
    type Stream;
    /// The builder method to produce a [WorkerBuilder] that will consume jobs
    fn with_storage(self, storage: ST) -> WorkerBuilder<Self::Job, Self::Stream, NS>;
}

pub struct AckJobLayer<ST, J> {
    storage: ST,
    job_type: PhantomData<J>,
    worker_id: WorkerId,
}

impl<ST, J> AckJobLayer<ST, J> {
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
            match res {
                Ok(_) => {
                    storage.ack(&worker_id, &job_id).await.ok();
                }
                _ => {}
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
    fn with_storage(
        mut self,
        mut storage: ST,
    ) -> WorkerBuilder<J, Self::Stream, Stack<AckJobLayer<ST, J>, M>> {
        let worker_id = self.id;
        let source = storage
            .consume(&worker_id, Duration::from_millis(10))
            .boxed();

        let layer = self.layer.layer(AckJobLayer {
            storage: storage.clone(),
            job_type: PhantomData,
            worker_id: worker_id.clone(),
        });

        let keep_alive: KeepAlive<ST, M> =
            KeepAlive::new::<J>(&worker_id, storage.clone(), Duration::from_secs(30));
        self.beats.push(Box::new(keep_alive));

        WorkerBuilder {
            job: PhantomData,
            layer,
            source,
            id: worker_id,
            beats: self.beats,
        }
    }
}
