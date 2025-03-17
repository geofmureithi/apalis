use std::{fmt::Debug, marker::PhantomData, time::Duration};

use apalis::prelude::*;

use apalis_redis::{self, Config};

use apalis_core::{
    codec::json::JsonCodec,
    layers::{Ack, AckLayer},
    response::Response,
};
use email_service::{send_email, Email};
use futures::{channel::mpsc, SinkExt};
use rsmq_async::{Rsmq, RsmqConnection, RsmqError};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::time::sleep;
use tracing::info;

struct RedisMq<T, C = JsonCodec<Vec<u8>>> {
    conn: Rsmq,
    msg_type: PhantomData<T>,
    config: Config,
    codec: PhantomData<C>,
}

type RedisMqMessage<Req> = (Req, RedisMqContext);

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct RedisMqContext {
    max_attempts: usize,
    message_id: String,
}

impl<Req> FromRequest<Request<Req, RedisMqContext>> for RedisMqContext {
    fn from_request(req: &Request<Req, RedisMqContext>) -> Result<Self, Error> {
        Ok(req.parts.context.clone())
    }
}

// Manually implement Clone for RedisMq
impl<T, C> Clone for RedisMq<T, C> {
    fn clone(&self) -> Self {
        RedisMq {
            conn: self.conn.clone(),
            msg_type: PhantomData,
            config: self.config.clone(),
            codec: self.codec,
        }
    }
}

impl<Req, C> Backend<Request<Req, RedisMqContext>> for RedisMq<Req, C>
where
    Req: Send + DeserializeOwned + 'static,
    C: Codec<Compact = Vec<u8>>,
{
    type Stream = RequestStream<Request<Req, RedisMqContext>>;

    type Layer = AckLayer<Self, Req, RedisMqContext, C>;

    type Codec = C;

    fn poll(mut self, _worker: &Worker<Context>) -> Poller<Self::Stream, Self::Layer> {
        let (mut tx, rx) = mpsc::channel(self.config.get_buffer_size());
        let stream: RequestStream<Request<Req, RedisMqContext>> = Box::pin(rx);
        let layer = AckLayer::new(self.clone());
        let heartbeat = async move {
            loop {
                sleep(*self.config.get_poll_interval()).await;
                let msg: Option<Request<Req, RedisMqContext>> = self
                    .conn
                    .receive_message(self.config.get_namespace(), None)
                    .await
                    .unwrap()
                    .map(|r| {
                        let mut req: Request<Req, RedisMqContext> =
                            C::decode(r.message).map_err(Into::into).unwrap();
                        req.parts.context.message_id = r.id;
                        req
                    });
                tx.send(Ok(msg)).await.unwrap();
            }
        };
        Poller::new_with_layer(stream, heartbeat, layer)
    }
}

impl<T, C, Res> Ack<T, Res, C> for RedisMq<T, C>
where
    T: Send,
    Res: Debug + Send + Sync,
    C: Send,
{
    type Context = RedisMqContext;

    type AckError = RsmqError;

    async fn ack(
        &mut self,
        ctx: &Self::Context,
        res: &Response<Res>,
    ) -> Result<(), Self::AckError> {
        if res.is_success() || res.attempt.current() >= ctx.max_attempts {
            self.conn
                .delete_message(self.config.get_namespace(), &ctx.message_id)
                .await?;
        }
        Ok(())
    }
}

impl<Message, C> MessageQueue<Message> for RedisMq<Message, C>
where
    Message: Send + Serialize + DeserializeOwned + 'static,
    C: Codec<Compact = Vec<u8>> + Send,
{
    type Error = RsmqError;

    type Compact = Vec<u8>;

    type Context = RedisMqContext;

    async fn enqueue(&mut self, message: Message) -> Result<(), Self::Error> {
        let bytes = C::encode(Request::<Message, RedisMqContext>::new(message))
            .map_err(Into::into)
            .unwrap();
        self.conn
            .send_message(self.config.get_namespace(), bytes, None)
            .await?;
        Ok(())
    }

    async fn enqueue_request(
        &mut self,
        message: Request<Message, RedisMqContext>,
    ) -> Result<(), Self::Error> {
        let bytes = C::encode(message).map_err(Into::into).unwrap();
        self.conn
            .send_message(self.config.get_namespace(), bytes, None)
            .await?;
        Ok(())
    }

    async fn enqueue_raw_request(
        &mut self,
        message: Request<Self::Compact, RedisMqContext>,
    ) -> Result<(), Self::Error> {
        let bytes = C::encode(message).map_err(Into::into).unwrap();
        self.conn
            .send_message(self.config.get_namespace(), bytes, None)
            .await?;
        Ok(())
    }

    async fn dequeue(&mut self) -> Result<Option<Message>, Self::Error> {
        Ok(self
            .conn
            .receive_message(self.config.get_namespace(), None)
            .await?
            .map(|r| {
                let req: Request<Message, RedisMqContext> =
                    C::decode(r.message).map_err(Into::into).unwrap();
                req.args
            }))
    }

    async fn dequeue_request(
        &mut self,
    ) -> Result<Option<Request<Message, RedisMqContext>>, Self::Error> {
        Ok(self
            .conn
            .receive_message(self.config.get_namespace(), None)
            .await?
            .map(|r| {
                let req: Request<Message, RedisMqContext> =
                    C::decode(r.message).map_err(Into::into).unwrap();
                req
            }))
    }

    async fn size(&mut self) -> Result<usize, Self::Error> {
        self.conn
            .get_queue_attributes(self.config.get_namespace())
            .await?
            .msgs
            .try_into()
            .map_err(|_| RsmqError::InvalidFormat("Could not convert to usize".to_owned()))
    }
}

async fn produce_jobs(mq: &mut RedisMq<Email>) -> anyhow::Result<()> {
    for index in 0..1 {
        mq.enqueue(Email {
            to: index.to_string(),
            text: "Test background job from apalis".to_string(),
            subject: "Background email job".to_string(),
        })
        .await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    std::env::set_var("RUST_LOG", "debug");

    tracing_subscriber::fmt::init();

    let mut conn = rsmq_async::Rsmq::new(Default::default()).await?;
    let _ = conn.create_queue("email", None, None, None).await;
    let mut mq = RedisMq {
        conn,
        msg_type: PhantomData,
        codec: PhantomData,
        config: Config::default().set_namespace("email"),
    };
    produce_jobs(&mut mq).await?;

    let worker = WorkerBuilder::new("rango-tango")
        .enable_tracing()
        .backend(mq)
        .build_fn(send_email);

    Monitor::new()
        .register(worker)
        .on_event(|e| info!("{e}"))
        .shutdown_timeout(Duration::from_millis(5000))
        .run_with_signal(async {
            tokio::signal::ctrl_c().await?;
            info!("Monitor starting shutdown");
            Ok(())
        })
        .await?;
    info!("Monitor shutdown complete");
    Ok(())
}
