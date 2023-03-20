use std::marker::PhantomData;

use std::time::Duration;
use futures::Future;
use tokio::time::interval;
use tower::{Layer, layer::util::Identity, Service};

use crate::{worker::WorkerRef, request::JobRequest};

use super::Storage;

pub struct KeepAliveLayer<T, Req> {
    worker: WorkerRef,
    storage: T,
    period: Duration,
    req_type: PhantomData<Req>,
}

impl<T, Req> KeepAliveLayer<T, Req> {
    /// Creates a new [`PeriodicLayer`] with the provided `make_request` closure
    /// and `period`.
    ///
    /// `make_request` returns a request to be called on the inner service.
    /// `period` gives with interval with which to send the request from `make_request`.
    pub fn new(worker: WorkerRef, storage: T, period: Duration) -> Self {
        KeepAliveLayer {
            worker,
            storage,
            period,
            req_type: PhantomData,
        }
    }
}

impl<S, T, F, Request> Layer<S> for KeepAliveLayer<T, Request>
where
    S: Service<JobRequest<Request>, Future = F> + Send + 'static,
    F: Future<Output = Result<S::Response, S::Error>> + Send + 'static,
    Request: Send,
    T: Storage<Output = Request> + Send + 'static,
{
    type Service = S;

    fn layer(&self, inner: S) -> Self::Service {
        let mut storage = self.storage.clone();
        let worker_id = self.worker.clone();
        let period = self.period;
        let make_worker = {
            let worker_id = worker_id.clone();
            let period = period;
            async move {
                let mut interval = interval(period);

                loop {
                    storage.keep_alive::<S>(worker_id.0.clone()).await.unwrap();
                    let _ = interval.tick().await;
                }
            }
        };
        tokio::spawn(make_worker);

        Layer::<S>::layer(&Identity::new(), inner)
    }
}
