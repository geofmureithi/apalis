use std::{error::Error, fmt::Debug};

use futures::{Future, Stream, StreamExt};

use tower::{Service, ServiceExt};
use tracing::info;
use tracing::warn;

use crate::executor::Executor;
use crate::job::Job;

use super::WorkerId;
use super::{Worker, WorkerContext, WorkerError};
use futures::future::FutureExt;
use std::fmt::Formatter;

/// A worker that is ready to consume jobs
pub struct ReadyWorker<Stream, Service> {
    pub(crate) id: WorkerId,
    pub(crate) stream: Stream,
    pub(crate) service: Service,
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
        Req: Send,
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
    async fn start<Exec: Executor + Send>(
        self,
        ctx: WorkerContext<Exec>,
    ) -> Result<(), WorkerError> {
        let mut service = self.service;
        let mut stream = ctx.shutdown.graceful_stream(self.stream);
        let (send, mut recv) = futures::channel::mpsc::channel::<()>(1);
        while let Some(res) = futures::select! {
            res = stream.next().fuse() => res,
            _ = ctx.shutdown.clone().fuse() => None
        } {
            let send_clone = send.clone();
            match res {
                Ok(Some(item)) => {
                    let svc = service
                        .ready()
                        .await
                        .map_err(|e| WorkerError::ServiceError(format!("{e:?}")))?;
                    let fut = svc.call(item);
                    ctx.spawn(async move {
                        fut.await;
                    });
                    drop(send_clone);
                }
                Err(e) => {
                    warn!("Error processing stream {e}");
                    drop(send_clone);
                }
                _ => {
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
