use crate::{
    error::{JobError, WorkerError},
    job::{Job, JobRequestWrapper, JobStreamResult},
    request::JobRequest,
    response::JobResult,
    worker::envelope::{Envelope, EnvelopeProxy, SyncEnvelopeProxy, ToEnvelope},
};
use futures::{Future, Stream, StreamExt};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
    time::Duration,
};
use tokio::{
    sync::{
        mpsc::{self, Receiver},
        oneshot::{self, error::RecvError},
    },
    task::{self, JoinHandle},
    time,
};
use tower::{Service, ServiceExt};
use tracing::Instrument;

use self::monitor::WorkerManagement;

#[cfg(feature = "broker")]
use {deadlock::WorkerId, std::any::type_name};

/// Allows communication between [Worker] and [Actor]
#[cfg(feature = "broker")]
pub mod broker;
#[cfg(feature = "broker")]
use broker::Broker;

#[cfg(feature = "broker")]
mod deadlock;
mod envelope;
mod monitor;

pub mod prelude {
    //! Module with most used items
    pub use super::{
        monitor::{Monitor, WorkerEvent, WorkerListener, WorkerMessage},
        Actor, Addr, AlwaysAddr, Context, ContextHandler, Handler, Message, Recipient, Worker,
        WorkerStatus,
    };
}

/// Represents the status of a [Worker].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum WorkerStatus {
    /// Worker is running fine
    Ok,
}

/// Worker trait
#[async_trait::async_trait]
pub trait Worker: Send + Sized + 'static {
    /// The job type for the worker
    type Job: Job;

    /// The tower service to process job requests
    type Service: Service<
            JobRequest<Self::Job>,
            Response = JobResult,
            Error = JobError,
            Future = Self::Future,
        > + Send;

    /// The future returened by our tower service
    type Future: Future<Output = Result<JobResult, JobError>> + Send;

    #[cfg(feature = "broker")]
    /// Used to bind broker
    /// TODO: Update this
    fn pre_start(&mut self, _broker: &Broker) {}

    /// At start hook of worker
    async fn on_start(&mut self, _ctx: &mut Context<Self>) {}

    /// At stop hook of worker
    async fn on_stop(&mut self, _ctx: &mut Context<Self>) {}

    /// Returns a streams of jobs for the worker to consume
    fn consume(&mut self) -> JobStreamResult<Self::Job>;

    /// Returns the service that handles the job
    fn service(&mut self) -> &mut Self::Service;

    /// The way the worker will handle a job
    async fn handle_job(&mut self, job: JobRequest<Self::Job>) -> Result<JobResult, JobError> {
        let handle = self.service().ready().await?;
        handle.call(job).await
    }

    /// Manage a worker
    async fn manage(&mut self, _msg: WorkerManagement) -> Result<WorkerStatus, WorkerError> {
        Ok(WorkerStatus::Ok)
    }
}

/// Address of actor. Can be used to send messages to it.
#[derive(Debug)]
pub struct Addr<A: Actor> {
    sender: mpsc::Sender<Envelope<A>>,
    #[cfg(feature = "broker")]
    actor_id: WorkerId,
}

impl<A: Actor> Clone for Addr<A> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            #[cfg(feature = "broker")]
            actor_id: self.actor_id,
        }
    }
}

impl<W: Actor> Addr<W> {
    fn new(sender: mpsc::Sender<Envelope<W>>) -> Self {
        Self {
            sender,
            #[cfg(feature = "broker")]
            actor_id: WorkerId::new(Some(type_name::<W>())),
        }
    }

    /// Send a message and wait for an answer.
    /// # Errors
    /// Fails if noone will send message
    /// # Panics
    /// If queue is full
    #[allow(unused_variables, clippy::expect_used)]
    pub async fn send<M>(&self, message: M) -> Result<M::Result, RecvError>
    where
        M: Message + Send + 'static,
        M::Result: Send,
        W: ContextHandler<M>,
    {
        let (sender, reciever) = oneshot::channel();
        let envelope = SyncEnvelopeProxy::pack(message, Some(sender));
        #[cfg(feature = "broker")]
        let from_actor_id_option = deadlock::task_local_actor_id();
        #[cfg(feature = "broker")]
        if let Some(from_actor_id) = from_actor_id_option {
            deadlock::r#in(self.actor_id, from_actor_id).await;
        }
        #[allow(clippy::todo)]
        if self.sender.send(envelope).await.is_err() {
            panic!("Queue is full. Handle this case");
        }
        let result = reciever.await;
        #[cfg(feature = "broker")]
        if let Some(from_actor_id) = from_actor_id_option {
            deadlock::out(self.actor_id, from_actor_id).await;
        }
        result
    }

    /// Send a message and wait for an answer.
    /// # Errors
    /// Fails if queue is full or actor is disconnected
    #[allow(clippy::result_unit_err)]
    pub async fn do_send<M>(&self, message: M) -> Option<M::Result>
    where
        M: Message + Send + 'static,
        M::Result: Send,
        W: ContextHandler<M>,
    {
        let (tx, rx) = oneshot::channel();
        let envelope = SyncEnvelopeProxy::pack(message, Some(tx));
        // TODO: propagate the error.
        let _error = self.sender.send(envelope).await;
        rx.await.ok()
    }

    /// Constructs recipient for sending only specific messages
    pub fn recipient<M>(&self) -> Recipient<M>
    where
        M: Message + Send + 'static,
        W: ContextHandler<M>,
        M::Result: Send,
    {
        Recipient(Box::new(self.clone()))
    }

    /// Contstructs address which will never panic on sending.
    ///
    /// Beware: You need to make sure that this actor will be always alive
    pub fn expect_running(self) -> AlwaysAddr<W> {
        AlwaysAddr(self)
    }
}

/// Address of an actor which is always alive.
#[derive(Debug)]
pub struct AlwaysAddr<A: Actor>(Addr<A>);

impl<A: Actor> Clone for AlwaysAddr<A> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<A: Actor> Deref for AlwaysAddr<A> {
    type Target = Addr<A>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<A: Actor> DerefMut for AlwaysAddr<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<A: Actor> AlwaysAddr<A> {
    /// Send a message and wait for an answer.
    pub async fn send<M>(&self, message: M) -> M::Result
    where
        M: Message + Send + 'static,
        M::Result: Send,
        A: ContextHandler<M>,
    {
        #[allow(clippy::expect_used)]
        self.deref()
            .send(message)
            .await
            .expect("Failed to get response from actor. It should have never failed!")
    }
}

// impl<M> From<mpsc::Sender<M>> for Recipient<M>
// where
//     M: Message + Send + 'static + Debug,
//     M::Result: Send,
// {
//     fn from(sender: mpsc::Sender<M>) -> Self {
//         Self(Box::new(sender))
//     }
// }

#[allow(missing_debug_implementations)]
/// Address of actor. Can be used to send messages to it.
pub struct Recipient<M: Message>(Box<dyn Sender<M> + Sync + Send + 'static>);

impl<M> std::fmt::Debug for Recipient<M>
where
    M: Message + Send,
{
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "Recipient {{ /* omitted */ }}")
    }
}

impl<M> Recipient<M>
where
    M: Message + Send,
    M::Result: Send,
{
    /// Send message to actor
    pub async fn send(&self, m: M) -> Option<M::Result> {
        self.0.send(m).await
    }
}

#[async_trait::async_trait]
trait Sender<M: Message> {
    async fn send(&self, m: M) -> Option<M::Result>;
}

#[async_trait::async_trait]
impl<A, M> Sender<M> for Addr<A>
where
    M: Message + Send + 'static,
    A: ContextHandler<M>,
    M::Result: Send,
{
    async fn send(&self, m: M) -> Option<M::Result> {
        self.do_send(m).await
    }
}

// #[async_trait::async_trait]
// impl<M> Sender<M> for mpsc::Sender<M>
// where
//     M: Message + Send + 'static + Debug,
//     M::Result: Send,
// {
//     async fn send(&self, m: M) {
//         let _result = self.send(m).await;
//     }
// }

#[async_trait::async_trait]
impl<T: 'static, W: 'static> Actor for W
where
    W: Worker<Job = T> + Handler<JobRequestWrapper<T>>,
    T: Send,
{
    /// At start hook of actor
    async fn on_start(&mut self, ctx: &mut Context<Self>) {
        <W as Worker>::on_start(self, ctx).await;
        let jobs = self.consume();
        let stream = jobs.map(|c| JobRequestWrapper(c));
        ctx.notify_with(stream);
    }

    /// At stop hook of actor
    async fn on_stop(&mut self, ctx: &mut Context<Self>) {
        <W as Worker>::on_stop(self, ctx).await;
    }
}

impl Message for WorkerManagement {
    type Result = Result<WorkerStatus, WorkerError>;
}

#[async_trait::async_trait]

impl<W> Handler<WorkerManagement> for W
where
    W: Worker + Actor,
{
    type Result = Result<WorkerStatus, WorkerError>;

    async fn handle(&mut self, msg: WorkerManagement) -> Result<WorkerStatus, WorkerError> {
        let res = self.manage(msg).await;
        res
    }
}
impl<T> Message for JobRequestWrapper<T> {
    type Result = ();
}

#[async_trait::async_trait]
impl<T, W> Handler<JobRequestWrapper<T>> for W
where
    W: Worker<Job = T> + 'static,
    T: Job + Serialize + Debug + DeserializeOwned + Send + 'static,
{
    type Result = ();
    async fn handle(&mut self, job: JobRequestWrapper<T>) -> Self::Result {
        match job.0 {
            Ok(Some(job)) => {
                self.handle_job(job).await.unwrap();
            }
            Ok(None) => {
                // on drain
            }
            Err(_e) => {
                todo!()
            }
        };
    }
}

/// Actor Trait
#[async_trait::async_trait]
pub trait Actor: Send + Sized + 'static {
    /// Capacity of worker queue
    fn mailbox_capacity(&self) -> usize {
        100
    }

    /// At start hook of actor
    async fn on_start(&mut self, _ctx: &mut Context<Self>) {}

    /// At stop hook of actor
    async fn on_stop(&mut self, _ctx: &mut Context<Self>) {}

    /// Initilize actor with its address.
    fn preinit(self) -> InitializedActor<Self> {
        let mailbox_capacity = self.mailbox_capacity();
        InitializedActor::new(self, mailbox_capacity)
    }

    /// Initialize actor with default values
    fn preinit_default() -> InitializedActor<Self>
    where
        Self: Default,
    {
        Self::default().preinit()
    }

    /// Starts an actor and returns its address
    async fn start(self) -> Addr<Self> {
        self.preinit().start().await
    }

    /// Starts an actor with default values and returns its address
    async fn start_default() -> Addr<Self>
    where
        Self: Default,
    {
        Self::default().start().await
    }
}

/// Initialized actor. Mainly used to take address before starting it.
#[derive(Debug)]
pub struct InitializedActor<A: Actor> {
    /// Address of actor
    pub address: Addr<A>,
    /// Worker itself
    pub actor: A,
    receiver: Receiver<Envelope<A>>,
}

impl<A: Actor> InitializedActor<A> {
    /// Constructor.
    pub fn new(actor: A, mailbox_capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel(mailbox_capacity);
        InitializedActor {
            actor,
            address: Addr::new(sender),
            receiver,
        }
    }

    /// Start Actor
    pub async fn start(self) -> Addr<A> {
        let address = self.address;
        let mut receiver = self.receiver;
        let mut actor = self.actor;
        let (handle_sender, handle_receiver) = oneshot::channel();
        let move_addr = address.clone();
        let actor_future = async move {
            #[allow(clippy::expect_used)]
            let join_handle = handle_receiver
                .await
                .expect("Unreachable as the message is always sent.");
            let mut ctx = Context::new(move_addr.clone(), join_handle);
            actor.on_start(&mut ctx).await;
            while let Some(Envelope(mut message)) = receiver.recv().await {
                EnvelopeProxy::handle(&mut *message, &mut actor, &mut ctx).await;
            }
            tracing::error!(actor = std::any::type_name::<A>(), "actor stopped");
            actor.on_stop(&mut ctx).await;
            // tokio::time::sleep(Duration::from_secs(1)).await;
            // Restart
        }
        .in_current_span();
        #[cfg(not(feature = "broker"))]
        let join_handle = task::spawn(actor_future);
        #[cfg(feature = "broker")]
        let join_handle = deadlock::spawn_task_with_actor_id(address.actor_id, actor_future);
        // TODO: propagate the error.
        let _error = handle_sender.send(join_handle);
        address
    }
}

/// Message trait for setting result of message
pub trait Message {
    /// Result type of message
    type Result: 'static;
}

/// Trait for actor for handling specific message type
#[async_trait::async_trait]
pub trait ContextHandler<M: Message>: Actor {
    /// Result of handler
    type Result: MessageResponse<M>;

    /// Message handler
    async fn handle(&mut self, ctx: &mut Context<Self>, msg: M) -> Self::Result;
}

/// Trait for actor for handling specific message type without context
#[async_trait::async_trait]
pub trait Handler<M: Message>: Actor {
    /// Result of handler
    type Result: MessageResponse<M>;

    /// Message handler
    async fn handle(&mut self, msg: M) -> Self::Result;
}

#[async_trait::async_trait]
impl<M: Message + Send + 'static, S: Handler<M>> ContextHandler<M> for S {
    type Result = S::Result;

    async fn handle(&mut self, _: &mut Context<Self>, msg: M) -> Self::Result {
        Handler::handle(self, msg).await
    }
}

/// Dev trait for Message responding
#[async_trait::async_trait]
pub trait MessageResponse<M: Message>: Send {
    /// Handles message
    async fn handle(self, sender: oneshot::Sender<M::Result>);
}

#[async_trait::async_trait]
impl<M> MessageResponse<M> for M::Result
where
    M: Message,
    M::Result: Send,
{
    async fn handle(self, sender: oneshot::Sender<M::Result>) {
        drop(sender.send(self));
    }
}

/// Context for execution of actor
#[derive(Debug)]
pub struct Context<W: Actor> {
    addr: Addr<W>,
    #[allow(dead_code)]
    handle: JoinHandle<()>,
}

impl<W: Actor> Context<W> {
    /// Default constructor
    pub fn new(addr: Addr<W>, handle: JoinHandle<()>) -> Self {
        Self { addr, handle }
    }

    /// Gets an address of current worker
    pub fn addr(&self) -> Addr<W> {
        self.addr.clone()
    }

    /// Gets an recipient for current worker with specified message type
    pub fn recipient<M>(&self) -> Recipient<M>
    where
        M: Message + Send + 'static,
        W: ContextHandler<M>,
        M::Result: Send,
    {
        self.addr().recipient()
    }

    /// Sends worker specified message
    pub fn notify<M>(&self, message: M)
    where
        M: Message + Send + 'static,
        W: ContextHandler<M>,
        M::Result: Send,
    {
        let addr = self.addr();
        drop(task::spawn(
            async move { addr.do_send(message).await }.in_current_span(),
        ));
    }

    /// Sends actor specified message in some time
    pub fn notify_later<M>(&self, message: M, later: Duration)
    where
        M: Message + Send + 'static,
        W: Handler<M>,
        M::Result: Send,
    {
        let addr = self.addr();
        drop(task::spawn(
            async move {
                time::sleep(later).await;
                addr.do_send(message).await
            }
            .in_current_span(),
        ));
    }

    /// Sends actor specified message in a loop with specified duration
    pub fn notify_every<M>(&self, every: Duration)
    where
        M: Message + Default + Send + 'static,
        W: Handler<M>,
        M::Result: Send,
    {
        let addr = self.addr();
        drop(task::spawn(
            async move {
                loop {
                    time::sleep(every).await;
                    let _res = addr.do_send(M::default()).await;
                }
            }
            .in_current_span(),
        ));
    }

    /// Notifies actor with items from stream
    pub fn notify_with<M, S>(&self, mut stream: S)
    where
        M: Message + Send + 'static,
        S: Stream<Item = M> + Unpin + Send + 'static,
        W: Handler<M>,
        M::Result: Send,
    {
        let addr = self.addr();
        drop(task::spawn(
            async move {
                while let Some(item) = stream.next().await {
                    addr.do_send(item).await;
                }
            }
            .in_current_span(),
        ));
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use futures::Future;
    use tower::{service_fn, Service, ServiceExt};

    use crate::error::WorkerError;
    use crate::worker::monitor::{Monitor, WorkerManagement};
    use crate::worker::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_simple_actor() {
        struct Job(String);

        impl Message for Job {
            type Result = Result<(), HandlerResult>;
        }

        #[derive(Debug)]
        struct HandlerResult;
        async fn handler2(msg: Job) -> Result<(), HandlerResult> {
            println!("Thread {:?} msg {:?}", std::thread::current().id(), msg.0);
            tokio::time::sleep(Duration::from_millis(10)).await;
            Err(HandlerResult)
        }

        struct TowerActor<S> {
            service: S,
        }

        #[async_trait::async_trait]
        impl<S, F> Actor for TowerActor<S>
        where
            S::Error: Debug,
            S: Service<Job, Response = (), Error = HandlerResult, Future = F> + Send + 'static,
            F: Future<Output = Result<(), HandlerResult>> + Send,
        {
            async fn on_start(&mut self, ctx: &mut Context<Self>) {
                use futures::stream;
                for i in 1..5 {
                    let stream = stream::iter(vec![
                        Job("17".to_string()),
                        Job("18".to_string()),
                        Job("19".to_string()),
                    ]);
                    ctx.notify_with(stream);
                }
            }
        }

        #[async_trait::async_trait]
        impl<S, F> Handler<Job> for TowerActor<S>
        where
            S::Error: Debug,
            S: Service<Job, Response = (), Error = HandlerResult, Future = F> + Send + 'static,
            F: Future<Output = Result<(), HandlerResult>> + Send,
        {
            type Result = Result<(), HandlerResult>;
            async fn handle(&mut self, msg: Job) -> Self::Result {
                let handle = self.service.ready().await.unwrap();
                let res = handle.call(msg).await;
                res
            }
        }

        let addr = TowerActor {
            service: service_fn(handler2),
        }
        .start()
        .await;
    }
}
