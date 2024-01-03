use std::{marker::PhantomData, time::Duration};

use crate::worker::{HeartBeat, WorkerId};

use super::Storage;

#[derive(Debug, Clone)]
pub(super) struct KeepAlive<ST, Service>(WorkerId, ST, Duration, PhantomData<Service>);

impl<ST, Service> KeepAlive<ST, Service> {
    pub(super) fn new<J: 'static>(
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

#[derive(Debug, Clone)]
pub(super) struct ReenqueueOrphaned<ST>(ST, i32, Duration, Duration);

impl<ST> ReenqueueOrphaned<ST> {
    pub(super) fn new<J: 'static>(
        storage: ST,
        count: i32,
        interval: Duration,
        timeout_worker: Duration,
    ) -> ReenqueueOrphaned<ST>
    where
        ST: Storage<Output = J>,
    {
        ReenqueueOrphaned(storage, count, interval, timeout_worker)
    }
}

#[async_trait::async_trait]
impl<S: Storage + Send + Sync> HeartBeat for ReenqueueOrphaned<S> {
    async fn heart_beat(&mut self) {
        match self
            .0
            .heartbeat(super::StorageWorkerPulse::ReenqueueOrphaned {
                count: self.1,
                timeout_worker: self.3,
            })
            .await
        {
            Err(e) => {
                tracing::warn!("An error occurred while attempting the reenqueue orphaned heartbeat. Error: {}", e)
            }
            _ => {
                tracing::trace!("reenqueue orphaned heartbeat successful for storage")
            }
        }
    }
    fn interval(&self) -> Duration {
        self.2
    }
}

#[derive(Debug, Clone)]
pub(super) struct EnqueueScheduled<ST>(ST, i32, Duration);

impl<ST> EnqueueScheduled<ST> {
    pub(super) fn new<J: 'static>(
        storage: ST,
        count: i32,
        interval: Duration,
    ) -> EnqueueScheduled<ST>
    where
        ST: Storage<Output = J>,
    {
        EnqueueScheduled(storage, count, interval)
    }
}

#[async_trait::async_trait]
impl<S: Storage + Send + Sync> HeartBeat for EnqueueScheduled<S> {
    async fn heart_beat(&mut self) {
        match self
            .0
            .heartbeat(super::StorageWorkerPulse::EnqueueScheduled { count: self.1 })
            .await
        {
            Err(e) => {
                tracing::warn!(
                    "An error occurred while attempting the enqueue scheduled heartbeat. Error: {}",
                    e
                )
            }
            _ => {
                tracing::trace!("enqueue scheduled heartbeat successful for storage")
            }
        }
    }
    fn interval(&self) -> Duration {
        self.2
    }
}
