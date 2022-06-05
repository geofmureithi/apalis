//! Module which is used for deadlock detection of actors

#![allow(clippy::expect_used, clippy::panic)]

use std::{
    cmp::{Eq, PartialEq},
    fmt::{self, Debug, Display},
    future::Future,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use once_cell::sync::Lazy;
use petgraph::graph::Graph;
use petgraph::{algo, graph::NodeIndex};
use tokio::{
    task::{self, JoinHandle},
    time,
};

use super::*;

static ACTOR_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

tokio::task_local! {
    static ACTOR_ID: WorkerId;
}

/// Spawns a task with task local [`WorkerId`].
pub(crate) fn spawn_task_with_actor_id<F>(actor_id: WorkerId, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    task::spawn(ACTOR_ID.scope(actor_id, future))
}

/// Gets task local [`WorkerId`] if this task has it or None.
pub(crate) fn task_local_actor_id() -> Option<WorkerId> {
    ACTOR_ID.try_with(|id| *id).ok()
}

#[derive(Clone, Copy)]
pub(crate) struct WorkerId {
    pub(crate) name: Option<&'static str>,
    pub(crate) id: usize,
}

impl WorkerId {
    pub(crate) fn new(name: Option<&'static str>) -> Self {
        Self {
            name,
            id: ACTOR_ID_COUNTER.fetch_add(1, Ordering::SeqCst),
        }
    }
}

impl Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(name) = &self.name {
            write!(f, "{}:{}", name, self.id)
        } else {
            write!(f, "<unknown>:{}", self.id)
        }
    }
}

impl Debug for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self, f)
    }
}

impl PartialEq for WorkerId {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for WorkerId {}

#[derive(Default)]
struct DeadlockActor(Graph<WorkerId, ()>);

impl Deref for DeadlockActor {
    type Target = Graph<WorkerId, ()>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for DeadlockActor {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

struct Reminder;
struct AddEdge {
    from: WorkerId,
    to: WorkerId,
}
struct RemoveEdge {
    from: WorkerId,
    to: WorkerId,
}
impl Message for Reminder {
    type Result = ();
}
impl Message for AddEdge {
    type Result = ();
}
impl Message for RemoveEdge {
    type Result = ();
}

impl DeadlockActor {
    fn find_or_create_from_to(
        &mut self,
        from: WorkerId,
        to: WorkerId,
    ) -> (NodeIndex<u32>, NodeIndex<u32>) {
        let (mut from_idx, mut to_idx) = (None, None);
        for i in self.node_indices() {
            if self[i] == from {
                from_idx = Some(i);
            } else if self[i] == to {
                to_idx = Some(i);
            }
        }
        (
            from_idx.unwrap_or_else(|| self.add_node(from)),
            to_idx.unwrap_or_else(|| self.add_node(to)),
        )
    }

    fn has_cycle(&self) -> bool {
        algo::is_cyclic_directed(&self.0)
    }
}

#[async_trait::async_trait]
impl Actor for DeadlockActor {
    async fn on_start(&mut self, ctx: &mut Context<Self>) {
        let recipient = ctx.recipient::<Reminder>();
        drop(task::spawn(async move {
            loop {
                recipient.send(Reminder).await;
                time::sleep(Duration::from_millis(100)).await
            }
        }));
    }
}

// Reminder for DeadlockWorker
#[async_trait::async_trait]
impl Handler<Reminder> for DeadlockActor {
    type Result = ();
    async fn handle(&mut self, _: Reminder) {
        if self.has_cycle() {
            panic!("Detected deadlock. Aborting. Cycle:\n{:#?}", self.0);
        }
    }
}

#[async_trait::async_trait]
impl Handler<AddEdge> for DeadlockActor {
    type Result = ();
    async fn handle(&mut self, AddEdge { from, to }: AddEdge) {
        let (from, to) = self.find_or_create_from_to(from, to);
        let _ = self.add_edge(from, to, ());
    }
}

#[async_trait::async_trait]
impl Handler<RemoveEdge> for DeadlockActor {
    type Result = ();
    async fn handle(&mut self, RemoveEdge { from, to }: RemoveEdge) {
        let (from, to) = self.find_or_create_from_to(from, to);
        let edge = self.find_edge(from, to).expect("Should be always present");
        let _ = self.remove_edge(edge);
    }
}

static DEADLOCK_ACTOR: Lazy<Addr<DeadlockActor>> = Lazy::new(|| {
    let actor = DeadlockActor::preinit_default();
    let address = actor.address.clone();
    let _result = task::spawn(actor.start());
    address
});

pub(crate) async fn r#in(to: WorkerId, from: WorkerId) {
    DEADLOCK_ACTOR.do_send(AddEdge { from, to }).await;
}

pub(crate) async fn out(to: WorkerId, from: WorkerId) {
    DEADLOCK_ACTOR.do_send(RemoveEdge { from, to }).await;
}
