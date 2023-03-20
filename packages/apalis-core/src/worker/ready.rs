use std::{fmt::Debug, error::Error};

use futures::{Stream, StreamExt, Future};
use log::info;
use tokio::sync::mpsc::channel;
use tower::{Service, ServiceExt};
use tracing::warn;

use crate::job::Job;

use super::{WorkerContext, Worker};

pub struct ReadyWorker<Stream, Service> {
    pub (crate) name: String,
    pub (crate) stream: Stream,
    pub (crate) layers: Service,
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
    type Error = E;
    async fn start(self, ctx: WorkerContext) -> Result<(), Self::Error> {
        let mut service = self.layers;
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
                    tokio::spawn(ctx.shutdown.graceful(async move {
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