#![allow(clippy::module_name_repetitions)]
use std::any::{Any, TypeId};
use std::sync::Arc;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use futures::future;
use once_cell::sync::OnceCell;

use super::*;

type TypeMap<V> = DashMap<TypeId, V>;
type BrokerRecipient = Box<dyn Any + Sync + Send + 'static>;
/// Broker type. Can be cloned and shared between many actors.
#[derive(Debug)]
pub struct Broker(Arc<TypeMap<Vec<(TypeId, BrokerRecipient)>>>);

static INSTANCE: OnceCell<Broker> = OnceCell::new();

impl Clone for Broker {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl Broker {
    /// Default constructor for broker
    pub fn global() -> &'static Broker {
        INSTANCE.get_or_init(|| Broker(Arc::new(DashMap::new())))
    }

    fn message_entry(&'_ self, id: TypeId) -> Entry<'_, TypeId, Vec<(TypeId, BrokerRecipient)>> {
        self.0.entry(id)
    }

    /// Send message via broker
    pub async fn issue_send<M: BrokerMessage + Send + Sync>(&self, m: M) {
        let entry = if let Entry::Occupied(entry) = self.message_entry(TypeId::of::<M>()) {
            entry
        } else {
            return;
        };
        let send = entry.get().iter().filter_map(|(_, recipient)| {
            recipient
                .downcast_ref::<Recipient<M>>()
                .map(|recipient| recipient.send(m.clone()))
        });
        drop(future::join_all(send).await);
    }

    fn subscribe_recipient<M: BrokerMessage>(&self, recipient: Recipient<M>) {
        let mut entry = self
            .message_entry(TypeId::of::<M>())
            .or_insert_with(|| Vec::with_capacity(1));
        if entry
            .iter()
            .any(|(actor_id, _)| *actor_id == TypeId::of::<Self>())
        {
            return;
        }
        entry.push((TypeId::of::<Self>(), Box::new(recipient)));
    }

    /// Subscribe actor to specific message type
    pub fn subscribe<M: BrokerMessage, A: Actor + ContextHandler<M>>(&self, ctx: &mut Context<A>) {
        self.subscribe_recipient(ctx.recipient::<M>())
    }

    // /// Subscribe with channel to specific message type
    // pub fn subscribe_with_channel<M: BrokerMessage + Debug>(&self) -> mpsc::Receiver<M> {
    //     let (sender, receiver) = mpsc::channel(100);
    //     // self.subscribe_recipient(sender.into());
    //     receiver
    // }

    /// Unsubscribe actor to this specific message type
    pub fn unsubscribe<M: BrokerMessage, A: Actor + ContextHandler<M>>(
        &self,
        _ctx: &mut Context<A>,
    ) {
        let mut entry = if let Entry::Occupied(entry) = self.message_entry(TypeId::of::<M>()) {
            entry
        } else {
            return;
        };

        if let Some(pos) = entry
            .get()
            .iter()
            .position(|(actor_id, _)| actor_id == &TypeId::of::<Self>())
        {
            drop(entry.get_mut().remove(pos));
        }
    }
}

/// Trait alias for messages which can be broked
pub trait BrokerMessage: Message<Result = ()> + Clone + 'static + Send {}

impl<M: Message<Result = ()> + Clone + 'static + Send> BrokerMessage for M {}
