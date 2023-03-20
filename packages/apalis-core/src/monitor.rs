use std::time::Duration;

use futures::{Future, FutureExt};
use graceful_shutdown::Shutdown;
use log::info;
use tokio::task::JoinHandle;
use tower::Service;
use tracing::warn;

use crate::{
    job::Job,
    request::JobRequest,
    worker::{Worker, WorkerContext},
};
pub struct Monitor {
    shutdown: Shutdown,
    worker_handles: Vec<(String, JoinHandle<()>)>,
    timeout: Option<Duration>,
}

impl Monitor {
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
        let name = "worker.name.clone()".to_string();
        let handle = tokio::spawn(
            self.shutdown
                .graceful(worker.start(WorkerContext { shutdown }).map(|_| ())),
        );
        self.worker_handles.push((name, handle));
        self
    }

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
}

impl Monitor {
    pub fn new() -> Self {
        Self {
            shutdown: Shutdown::new(),
            worker_handles: Vec::new(),
            timeout: None,
        }
    }

    pub fn shutdown_timeout(mut self, duration: Duration) -> Self {
        self.timeout = Some(duration);
        self
    }

    pub async fn run_with_signal<S: Future<Output = std::io::Result<()>>>(
        self,
        signal: S,
    ) -> std::io::Result<()> {
        self.shutdown.shutdown_after(signal).await?;
        info!("Shutting down the system");
        self.run().await?;
        Ok(())
    }

    pub async fn run(self) -> std::io::Result<()> {
        let _res: Vec<(String, bool)> = self
            .worker_handles
            .into_iter()
            .map(|h| (h.0, h.1.is_finished()))
            .collect();
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
