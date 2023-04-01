use std::{marker::PhantomData, time::Duration};

use crate::worker::{HeartBeat, WorkerId};

use super::Storage;

#[derive(Debug, Clone)]
pub (super) struct KeepAlive<ST, Service>(WorkerId, ST, Duration, PhantomData<Service>);

impl<ST, Service> KeepAlive<ST, Service> {
    pub (super) fn new<J: 'static>(
        worker_id: &WorkerId,
        storage: ST,
        interval: Duration,
    ) -> KeepAlive<ST, Service>
    where
        ST: Storage<Output = J>,
    {
        KeepAlive(worker_id.clone(), storage, interval, PhantomData)
    }
}

#[async_trait::async_trait]
impl<S: Storage + Send + Sync, Service: Sync + Send> HeartBeat for KeepAlive<S, Service> {
    async fn heart_beat(&mut self) {
        match self.1.keep_alive::<Service>(&self.0).await {
            Err(e) => {
                tracing::warn!("An error occurred while attempting the keep alive heartbeat for worker {}. Error: {}", self.0, e)
            }
            _ => {
                tracing::trace!("keep alive heartbeat successful for worker: {}", self.0)
            }
        }
    }
    fn interval(&self) -> Duration {
        self.2
    }
}
