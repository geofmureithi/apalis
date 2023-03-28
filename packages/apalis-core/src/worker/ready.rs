use std::{error::Error, fmt::Debug};

use futures::{Future, Stream, StreamExt};
use tokio::sync::mpsc::channel;
use tower::{Service, ServiceExt};
use tracing::info;
use tracing::warn;

use crate::executor::Executor;
use crate::job::Job;

use super::{Worker, WorkerContext};
use std::fmt::Formatter;

/// A worker that is ready to consume jobs
pub struct ReadyWorker<Stream, Service> {
    pub(crate) name: String,
    pub(crate) stream: Stream,
    pub(crate) service: Service,
}

impl<Stream, Service> Debug for ReadyWorker<Stream, Service> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadyWorker")
            .field("name", &self.name)
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

    fn name(&self) -> String {
        self.name.to_string()
    }
    async fn start<Exec: Executor + Send>(
        self,
        ctx: WorkerContext<Exec>,
    ) -> Result<(), super::WorkerError> {
        let mut service = self.service;
        let mut stream = ctx.shutdown.graceful_stream(self.stream);
        let (send, mut recv) = channel::<()>(1);

        while let Some(res) = tokio::select! {
            res = stream.next() => res,
            _ = ctx.shutdown.clone() => None
        } {
            let send_clone = send.clone();
            match res {
                Ok(Some(item)) => {
                    let svc = service.ready().await.unwrap();
                    let fut = svc.call(item);
                    ctx.executor.spawn(ctx.shutdown.graceful(async move {
                        fut.await;
                    }));
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
        info!("Shutting down {} worker", self.name);
        let _ = recv.recv().await;
        info!("Shutdown {} worker successfully", self.name);
        Ok(())
    }
}
