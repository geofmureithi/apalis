use std::marker::PhantomData;

use futures::{future::BoxFuture, FutureExt};
use tower::{Layer, Service};

use crate::{error::BoxDynError, request::JobRequest, worker::WorkerId, context::HasJobContext};

/// An error occurred while trying to acknowledge a message.
#[derive(Debug, thiserror::Error)]
pub enum AckError {
    /// Acknowledgement failed 
    #[error("Acknowledgement failed {0}")]
    NoAck(#[source] BoxDynError),
}

/// A trait for acknowledging successful job processing
#[async_trait::async_trait]
pub trait Ack<J> {
    /// The data to fetch from context to allow acknowledgement, usually [`JobId`]
    type Acknowledger;
    /// Acknowledges successful processing of the given request
    async fn ack(&self, worker_id: &WorkerId, data: &Self::Acknowledger) -> Result<(), AckError>;
}

/// A layer that acknowledges a job completed successfully
#[derive(Debug)]
pub struct AckLayer<A: Ack<J>, J> {
    ack: A,
    job_type: PhantomData<J>,
    worker_id: WorkerId,
}

impl<A: Ack<J>, J> AckLayer<A, J> {
    /// Build a new [AckLayer] for a job
    pub fn new(ack: A, worker_id: WorkerId) -> Self {
        Self {
            ack,
            job_type: PhantomData,
            worker_id,
        }
    }
}

impl<A, J, S> Layer<S> for AckLayer<A, J>
where
    S: Service<JobRequest<J>> + Send + 'static,
    S::Error: std::error::Error + Send + Sync + 'static,
    S::Future: Send + 'static,
    A: Ack<J> + Clone + Send + Sync + 'static,
{
    type Service = AckService<S, A, J>;

    fn layer(&self, service: S) -> Self::Service {
        AckService {
            service,
            ack: self.ack.clone(),
            job_type: PhantomData,
            worker_id: self.worker_id.clone(),
        }
    }
}

/// The underlying service for an [AckLayer]
#[derive(Debug)]
pub struct AckService<SV, A, J> {
    service: SV,
    ack: A,
    job_type: PhantomData<J>,
    worker_id: WorkerId,
}

impl<SV, A, J> Service<JobRequest<J>> for AckService<SV, A, J>
where
    SV: Service<JobRequest<J>> + Send + Sync + 'static,
    SV::Error: std::error::Error + Send + Sync + 'static,
    <SV as Service<JobRequest<J>>>::Future: std::marker::Send + 'static,
    A: Ack<J> + Send + 'static + Clone + Send + Sync,
    J: 'static,
    <SV as Service<JobRequest<J>>>::Response: std::marker::Send,
    <A as Ack<J>>::Acknowledger: Sync + Send + Clone,
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
        let ack = self.ack.clone();
        let worker_id = self.worker_id.clone();
        let data = request.context().data::<<A as Ack<J>>::Acknowledger>().unwrap().clone();
        
        let fut = self.service.call(request);
        let fut_with_ack = async move {
            let res = fut.await;
            if res.is_ok() {
                if let Err(e) =ack.ack(&worker_id.clone(), &data).await  {
                    tracing::warn!("Acknowledgement Failed: {}", e); 
                }
            }
            res
        };
        fut_with_ack.boxed()
    }
}
