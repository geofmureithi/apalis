#![allow(clippy::module_name_repetitions)]

use tokio::sync::oneshot;

use super::*;

pub(crate) trait ToEnvelope<A: Actor + ContextHandler<M>, M: Message> {
    fn pack(msg: M, channel: Option<oneshot::Sender<M::Result>>) -> Envelope<A>;
}

pub(crate) struct Envelope<A: Actor>(pub(crate) Box<dyn EnvelopeProxy<A> + Send>);

impl<A, M> ToEnvelope<A, M> for SyncEnvelopeProxy<M>
where
    A: Actor + ContextHandler<M>,
    M: Message + Send + 'static,
    M::Result: Send,
{
    fn pack(msg: M, channel: Option<oneshot::Sender<M::Result>>) -> Envelope<A> {
        Envelope(Box::new(Self {
            msg: Some(msg),
            channel,
        }))
    }
}

#[async_trait::async_trait]
pub(crate) trait EnvelopeProxy<A: Actor> {
    async fn handle(&mut self, actor: &mut A, ctx: &mut Context<A>);
}

pub(crate) struct SyncEnvelopeProxy<M>
where
    M: Message + Send,
    M::Result: Send,
{
    msg: Option<M>,
    channel: Option<oneshot::Sender<M::Result>>,
}

#[async_trait::async_trait]
impl<A, M> EnvelopeProxy<A> for SyncEnvelopeProxy<M>
where
    A: Actor + ContextHandler<M> + Send,
    M: Message + Send,
    M::Result: Send,
{
    async fn handle(&mut self, actor: &mut A, ctx: &mut Context<A>) {
        match (self.channel.take(), self.msg.take()) {
            (Some(channel), Some(msg)) => actor.handle(ctx, msg).await.handle(channel).await,
            (None, Some(msg)) => drop(actor.handle(ctx, msg).await),
            (_, None) => unreachable!(),
        }
    }
}
