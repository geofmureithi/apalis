use std::{
    fmt::{self, Debug, Formatter},
    time::Duration,
};

use futures::{Future, FutureExt};
use graceful_shutdown::Shutdown;
use log::info;
use tower::Service;
use tracing::warn;

use crate::{
    executor::Executor,
    job::Job,
    request::JobRequest,
    worker::{Worker, WorkerContext, WorkerId},
};

/// A monitor for coordinating and managing a collection of workers.
pub struct Monitor<E> {
    shutdown: Shutdown,
    worker_handles: Vec<WorkerId>,
    timeout: Option<Duration>,
    executor: E,
}

impl<E: Executor> Debug for Monitor<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Monitor")
            .field("shutdown", &"[Graceful shutdown listener]")
            .field("worker_handles", &self.worker_handles.iter().cloned())
            .field("timeout", &self.timeout)
            .field("executor", &std::any::type_name::<E>())
            .finish()
    }
}

impl<E: Executor + Send + Sync + 'static> Monitor<E> {
    /// Registers a worker with the monitor.
    ///
    /// # Arguments
    ///
    /// * `worker` - The worker to register.
    ///
    /// # Returns
    ///
    /// The monitor instance, with the worker added to the collection.
    pub fn register<
        Strm,
        Serv: Service<JobRequest<J>>,
        J: Job + 'static,
        W: Worker<J, Service = Serv, Source = Strm> + 'static,
    >(
        mut self,
        worker: W,
    ) -> Self
    where
        <Serv as Service<JobRequest<J>>>::Future: std::marker::Send,
    {
        let shutdown = self.shutdown.clone();
        let worker_id = worker.id();
        self.executor.spawn(
            self.shutdown.graceful(
                worker
                    .start(WorkerContext {
                        shutdown,
                        executor: self.executor.clone(),
                        worker_id: worker_id.clone(),
                    })
                    .map(|_| ()),
            ),
        );
        self.worker_handles.push(worker_id);
        self
    }

    /// Registers multiple workers with the monitor.
    ///
    /// # Arguments
    ///
    /// * `count` - The number of workers to register.
    /// * `caller` - A function that returns a new worker instance for each index.
    ///
    /// # Returns
    ///
    /// The monitor instance, with all workers added to the collection.
    ///
    pub fn register_with_count<
        Strm,
        Serv: Service<JobRequest<J>>,
        J: Job + 'static,
        W: Worker<J, Service = Serv, Source = Strm> + 'static,
        Call: Fn(u16) -> W,
    >(
        mut self,
        count: u16,
        caller: Call,
    ) -> Self
    where
        <Serv as Service<JobRequest<J>>>::Future: std::marker::Send,
    {
        for index in 0..count {
            let worker = caller(index);
            self = self.register(worker);
        }

        self
    }

    /// Sets a timeout duration for the monitor's shutdown process.
    ///
    /// # Arguments
    ///
    /// * `duration` - The timeout duration.
    ///
    /// # Returns
    ///
    /// The monitor instance, with the shutdown timeout duration set.
    ///
    pub fn shutdown_timeout(mut self, duration: Duration) -> Self {
        self.timeout = Some(duration);
        self
    }

    /// Runs the monitor and all its registered workers until they have all completed or a shutdown signal is received.
    ///
    /// # Arguments
    ///
    /// * `signal` - A `Future` that resolves when a shutdown signal is received.
    ///
    /// # Errors
    ///
    /// If the monitor fails to shutdown gracefully, an `std::io::Error` will be returned.

    pub async fn run_with_signal<S: Future<Output = std::io::Result<()>>>(
        self,
        signal: S,
    ) -> std::io::Result<()> {
        self.shutdown.shutdown_after(signal).await?;
        info!("Shutting down the system");
        self.run().await?;
        Ok(())
    }

    /// Runs the monitor and all its registered workers until they have all completed.
    ///
    /// # Errors
    ///
    /// If the monitor fails to shutdown gracefully, an `std::io::Error` will be returned.
    ///
    /// # Remarks
    ///
    /// If a timeout has been set using the `shutdown_timeout` method, the monitor
    /// will wait for all workers to complete up to the timeout duration before exiting.
    /// If the timeout is reached and workers have not completed, the monitor will log a warning
    /// message and exit forcefully.
    pub async fn run(self) -> std::io::Result<()> {
        if let Some(timeout) = self.timeout {
            if self.shutdown.with_timeout(timeout).await {
                warn!("Shutdown timeout reached. Exiting forcefully");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Shutdown timeout reached. Exiting forcefully",
                ));
            }
        } else {
            self.shutdown.await;
        }
        info!("Successfully shutdown monitor and all workers");
        Ok(())
    }
}

impl Default for Monitor<()> {
    fn default() -> Self {
        Self::new()
    }
}

impl Monitor<()> {
    /// Creates a new monitor instance.
    ///
    /// # Returns
    ///
    /// A new monitor instance, with an empty collection of workers.
    pub fn new() -> Self {
        Self {
            shutdown: Shutdown::new(),
            worker_handles: Vec::new(),
            timeout: None,
            executor: (),
        }
    }

    /// Sets a custom executor for the monitor, allowing the usage of another runtime apart from Tokio.
    /// The executor must implement the `Executor` trait.
    pub fn executor<E: Executor>(self, executor: E) -> Monitor<E> {
        Monitor {
            shutdown: self.shutdown,
            worker_handles: Vec::new(),
            timeout: self.timeout,
            executor,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use crate::{context::JobContext, job_fn::job_fn, worker::WorkerError};

    use super::*;
    use futures::Stream;
    use tokio::time::sleep;
    use tower::ServiceBuilder;

    struct TestJob {}

    impl Job for TestJob {
        const NAME: &'static str = "TestJob";
    }

    struct TestWorker<S> {
        _service: S,
    }

    async fn test_service(_req: TestJob, _ctx: JobContext) {}

    #[async_trait::async_trait]
    impl<S: Send> Worker<TestJob> for TestWorker<S> {
        type Service = S;
        type Source = TestSource;

        fn id(&self) -> WorkerId {
            WorkerId::new("test-worker")
        }

        async fn start<E: Executor + Send>(
            self,
            _ctx: WorkerContext<E>,
        ) -> Result<(), WorkerError> {
            sleep(Duration::from_millis(100)).await;
            Ok(())
        }
    }
    struct TestSource {}

    impl Stream for TestSource {
        type Item = Result<TestJob, ()>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    #[tokio::test]
    async fn test_monitor_run() {
        let monitor = Monitor::new()
            .register(TestWorker {
                _service: ServiceBuilder::new().service(job_fn(test_service)),
            })
            .shutdown_timeout(Duration::from_secs(1));
        let shutdown = monitor.shutdown.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(500)).await;
            shutdown.shutdown();
        });

        let result = monitor.run().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_monitor_run_with_signal() {
        let monitor = Monitor::new()
            .register(TestWorker {
                _service: ServiceBuilder::new().service(job_fn(test_service)),
            })
            .shutdown_timeout(Duration::from_secs(1));
        let shutdown = monitor.shutdown.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(500)).await;
            shutdown.shutdown();
        });

        let result = monitor.run_with_signal(async { Ok(()) }).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_monitor_register() {
        let monitor = Monitor::new();
        assert_eq!(monitor.worker_handles.len(), 0);

        let monitor = monitor.register(TestWorker {
            _service: ServiceBuilder::new().service(job_fn(test_service)),
        });
        assert_eq!(monitor.worker_handles.len(), 1);
    }

    #[tokio::test]
    async fn test_monitor_register_with_count() {
        let monitor = Monitor::new();
        assert_eq!(monitor.worker_handles.len(), 0);

        let monitor = monitor.register_with_count(5, |_| TestWorker {
            _service: ServiceBuilder::new().service(job_fn(test_service)),
        });
        assert_eq!(monitor.worker_handles.len(), 5);
    }
}
