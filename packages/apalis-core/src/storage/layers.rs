use std::marker::PhantomData;

use futures::Future;
use std::time::Duration;
use tokio::time::interval;
use tower::{layer::util::Identity, Layer, Service};

use crate::{request::JobRequest, worker::WorkerRef};

use super::Storage;

/// A `tower::layer::Layer` that wraps a service to periodically send a "keep-alive" message
/// to the source to notify it that the worker is still alive. This layer keeps a reference to
/// the worker and its name and uses it to send the "keep-alive" message.
pub struct KeepAliveLayer<T, Req> {
    worker: WorkerRef,
    storage: T,
    period: Duration,
    req_type: PhantomData<Req>,
}

impl<T, Req> std::fmt::Debug for KeepAliveLayer<T, Req> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeepAliveLayer")
            .field("worker_name", &self.worker.name())
            .field("period", &self.period)
            .finish()
    }
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
        let worker_ref = self.worker.clone();
        let period = self.period;
        let make_worker = {
            let period = period;
            async move {
                let mut interval = interval(period);

                loop {
                    storage
                        .keep_alive::<S>(worker_ref.name().to_string())
                        .await
                        .unwrap();
                    let _ = interval.tick().await;
                }
            }
        };
        tokio::spawn(make_worker);

        Layer::<S>::layer(&Identity::new(), inner)
    }
}
