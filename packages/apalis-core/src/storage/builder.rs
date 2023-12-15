use crate::layers::ack::AckLayer;
use crate::layers::extensions::Extension;
use futures::StreamExt;
use std::{marker::PhantomData, time::Duration};
use tower::layer::util::Stack;

use crate::{builder::WorkerBuilder, job::JobStreamResult};

use super::beats::EnqueueScheduled;
use super::beats::ReenqueueOrphaned;
use super::{beats::KeepAlive, Storage};

/// A helper trait to help build a [WorkerBuilder] that consumes a [Storage]
pub trait WithStorage<NS, ST: Storage<Output = Self::Job>>: Sized {
    /// The job to consume
    type Job;
    /// The source of jobs
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
    reenqueue_orphaned: Option<(i32, Duration, Duration)>,
    buffer_size: usize,
    fetch_interval: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            keep_alive: Duration::from_secs(30),
            enqueue_scheduled: Some((10, Duration::from_secs(10))),
            reenqueue_orphaned: Some((10, Duration::from_secs(10), Duration::from_secs(300))),
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
    pub fn reenqueue_orphaned(mut self, interval: Option<(i32, Duration, Duration)>) -> Self {
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

impl<J: 'static + Send + Sync, M, ST>
    WithStorage<Stack<Extension<ST>, Stack<AckLayer<ST, J>, M>>, ST> for WorkerBuilder<(), (), M>
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
    ) -> WorkerBuilder<J, Self::Stream, Stack<Extension<ST>, Stack<AckLayer<ST, J>, M>>> {
        let worker_config = config(WorkerConfig::default());
        let worker_id = self.id;
        let source = storage
            .consume(
                &worker_id,
                worker_config.fetch_interval,
                worker_config.buffer_size,
            )
            .boxed();

        let layer = self
            .layer
            .layer(AckLayer::new(storage.clone(), worker_id.clone()))
            .layer(Extension(storage.clone()));

        let keep_alive: KeepAlive<ST, M> =
            KeepAlive::new::<J>(&worker_id, storage.clone(), worker_config.keep_alive);
        self.beats.push(Box::new(keep_alive));
        if let Some((count, duration, timeout_worker)) = worker_config.reenqueue_orphaned {
            let reenqueue_orphaned =
                ReenqueueOrphaned::new(storage.clone(), count, duration, timeout_worker);
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
