use std::{error::Error, fmt::Debug};

use futures::{Future, Stream, StreamExt};

use tower::ServiceBuilder;
use tower::{Service, ServiceExt};
use tracing::info;
use tracing::warn;

use crate::context::HasJobContext;
use crate::executor::Executor;
use crate::job::Job;
#[cfg(feature = "extensions")]
use crate::layers::extensions::Extension;
use crate::utils::Timer;

use super::HeartBeat;
use super::WorkerId;
use super::{Worker, WorkerContext, WorkerError};
use futures::future::{join_all, FutureExt};
use std::fmt::Formatter;

/// A worker that is ready to consume jobs
pub struct ReadyWorker<Stream, Service> {
    pub(crate) id: WorkerId,
    pub(crate) stream: Stream,
    pub(crate) service: Service,
    pub(crate) beats: Vec<Box<dyn HeartBeat + Send>>,
    pub(crate) max_concurrent_jobs: usize,
}

impl<Stream, Service> Debug for ReadyWorker<Stream, Service> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadyWorker")
            .field("name", &self.id)
            .field("stream", &std::any::type_name::<Stream>())
            .field("service", &std::any::type_name::<Service>())
            .finish()
    }
}

#[async_trait::async_trait]
impl<
        Strm: Unpin + Send + Stream<Item = Result<Option<Req>, E>> + 'static,
        Serv: Service<Req, Future = Fut> + Send + 'static,
        J: Job + 'static,
        E: 'static + Send + Error + Sync,
        Req: Send + HasJobContext,
        Fut: Future + Send + 'static,
    > Worker<J> for ReadyWorker<Strm, Serv>
where
    <Serv as Service<Req>>::Error: Debug,
{
    type Service = Serv;
    type Source = Strm;

    fn id(&self) -> WorkerId {
        self.id.clone()
    }

    async fn start<Exec: Executor + Send + Sync + 'static>(
        self,
        ctx: WorkerContext<Exec>,
    ) -> Result<(), WorkerError> {
        #[cfg(feature = "extensions")]
        let mut service = ServiceBuilder::new()
            .layer(Extension(ctx.clone()))
            .service(self.service);
        #[cfg(not(feature = "extensions"))]
        let mut service = self.service;
        let mut stream = ctx
            .shutdown
            .graceful_stream(self.stream)
            .ready_chunks(self.max_concurrent_jobs);
        let (send, mut recv) = futures::channel::mpsc::channel::<()>(1);
        // Setup any heartbeats by the worker
        for mut beat in self.beats {
            ctx.executor.spawn(async move {
                #[cfg(feature = "async-std-comp")]
                #[allow(unused_variables)]
                let sleeper = crate::utils::timer::AsyncStdTimer;
                #[cfg(feature = "tokio-comp")]
                let sleeper = crate::utils::timer::TokioTimer;
                loop {
                    let interval = beat.interval();
                    beat.heart_beat().await;
                    sleeper.sleep(interval).await;
                }
            });
        }
        while let Some(res) = futures::select_biased! {
            _ = ctx.shutdown.clone().fuse() => None,
            res = stream.next().fuse() => res,
        } {
            let res: Result<Vec<_>, _> = res.into_iter().collect();
            let send_clone = send.clone();
            match res {
                Ok(items) => {
                    let mut futures = Vec::with_capacity(items.len());
                    let items = items.into_iter().flatten();
                    for item in items {
                        let svc = service
                            .ready()
                            .await
                            .map_err(|e| WorkerError::ServiceError(format!("{e:?}")))?;
                        let fut = svc.call(item);
                        let fut = async move {
                            fut.await;
                        };
                        futures.push(fut);
                    }
                    join_all(futures).await;
                    drop(send_clone);
                }
                Err(e) => {
                    warn!("Error processing stream {e}");
                    drop(send_clone);
                }
            }
        }
        drop(send);
        info!("Shutting down {} worker", self.id);
        let _ = recv.next().await;
        info!("Shutdown {} worker successfully", self.id);
        Ok(())
    }
}
