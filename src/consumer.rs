use crate::error::TaskError as Error;
use crate::message::MessageDecodable;
use crate::message::MessageEncodable;
use crate::queue::Queue;
use crate::storage::redis::RedisStorage;
use actix::clock::{interval_at, Duration, Instant};
use actix::prelude::*;
use actix::{Actor, Context, Handler, Message};
use backoff::future::retry_notify;
use backoff::ExponentialBackoff;
use chrono::prelude::*;
use futures::future::BoxFuture;
use futures::stream::StreamExt;
use log::{debug, error, info, warn};
use redis::aio::MultiplexedConnection;
use redis::Value;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::ops::{Deref, Drop};
use std::sync::{Arc, Mutex};

#[derive(Message)]
#[rtype(result = "()")]
struct HeartBeat;

#[derive(Message)]
#[rtype(result = "()")]
struct Stop;

#[derive(Message)]
#[rtype(result = "()")]
struct Schedule;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum JobState {
    Unacked,
    Acked,
    Rejected,
}

#[derive(Debug)]
pub enum JobResult {
    Result(Result<(), Error>),
    Retry(Error),
}

#[derive(Serialize, Deserialize)]
pub struct Job<T> {
    pub id: uuid::Uuid,
    message: T,
    state: JobState,
    retries: i64,
}

pub trait JobHandler {
    fn handle(&self, ctx: &JobContext) -> BoxFuture<JobResult>;
}

impl<T: 'static + JobHandler> Job<T>
where
    T: JobHandler,
{
    pub fn new(message: T) -> Job<T> {
        Job {
            id: uuid::Uuid::new_v4(),
            message,
            state: JobState::Unacked,
            retries: 0,
        }
    }

    async fn handle(&self, ctx: &JobContext) -> Result<(), Error> {
        let res = self.message.handle(ctx).await;
        match res {
            JobResult::Result(r) => Ok(()),
            JobResult::Retry(e) => Err(e),
        }
    }

    pub fn retry(&mut self) {
        self.state = JobState::Acked;
        self.retries = self.retries + 1;
    }

    pub fn ack(&mut self) {
        self.state = JobState::Acked;
    }

    pub fn reject(&mut self) {
        self.state = JobState::Rejected;
    }
}

impl<T> Drop for Job<T> {
    fn drop(&mut self) {
        if self.state == JobState::Unacked {
            warn!("Dropping Unacked Job");
        }
    }
}

use fnv::FnvHashMap;
use std::any::{Any, TypeId};
use std::fmt::{self, Debug, Formatter};

#[derive(Default)]
pub struct JobContext(FnvHashMap<TypeId, Box<dyn Any + Sync + Send>>);

impl Deref for JobContext {
    type Target = FnvHashMap<TypeId, Box<dyn Any + Sync + Send>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl JobContext {
    fn insert<D: Any + Send + Sync>(&mut self, data: D) {
        self.0.insert(TypeId::of::<D>(), Box::new(data));
    }
    pub fn data_opt<D: Any + Send + Sync>(&self) -> Option<&D> {
        self.0
            .get(&TypeId::of::<D>())
            .and_then(|d| d.downcast_ref::<D>())
    }
}

impl Debug for JobContext {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_tuple("Data").finish()
    }
}

pub struct Consumer<M: JobHandler> {
    data: Arc<Mutex<JobContext>>,
    storage: RedisStorage,
    queue: Queue,
    id: String,
    typeid: PhantomData<M>,
}

impl<M: JobHandler> Consumer<M> {
    pub fn new_with_conn(conn: MultiplexedConnection, queue: Queue) -> Self {
        let id = uuid::Uuid::new_v4().to_string();
        Consumer {
            data: Arc::default(),
            storage: RedisStorage { conn },
            queue,
            id,
            typeid: PhantomData,
        }
    }
    pub fn new<S: Into<String>>(redis: &RedisStorage, queue: S) -> Self {
        let id = uuid::Uuid::new_v4().to_string();
        let queue = Queue::new(&queue.into());
        Consumer {
            data: Arc::default(),
            storage: redis.clone(),
            queue,
            id,
            typeid: PhantomData,
        }
    }

    pub fn data<T: 'static + Sync + Send>(self, data: T) -> Self {
        self.data.lock().unwrap().insert::<T>(data);
        self
    }
}

impl<M: 'static + std::marker::Unpin + JobHandler + serde::de::DeserializeOwned + Serialize>
    StreamHandler<HeartBeat> for Consumer<M>
{
    fn handle(&mut self, _: HeartBeat, _: &mut Context<Consumer<M>>) {
        debug!("Received heartbeat for consumer: {:?}", self.id);
    }

    fn finished(&mut self, _: &mut Self::Context) {
        warn!("Heartbeat for consumer: {:?} stopped", self.id);
    }
}

/// Actix message implements request Redis to Enqueue scheduled jobs
impl<M: 'static + std::marker::Unpin + JobHandler + serde::de::DeserializeOwned + Serialize>
    StreamHandler<Schedule> for Consumer<M>
{
    fn handle(&mut self, _: Schedule, ctx: &mut Context<Consumer<M>>) {
        let mut con = self.storage.conn.clone();
        let enqueue_jobs = redis::Script::new(include_str!("lua/enqueue_scheduled_jobs.lua"));
        let scheduled_jobs_set = self.queue.scheduled_jobs_set.to_string();
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let signal_list = self.queue.signal_list.to_string();
        let timestamp = Utc::now().timestamp();
        let fut = async move {
            let res: Result<i8, Error> = enqueue_jobs
                .key(scheduled_jobs_set)
                .key(active_jobs_list)
                .key(signal_list)
                .arg(timestamp)
                .arg("10".to_string()) //Enque 10 jobs
                .invoke_async(&mut con)
                .await
                .map_err(Error::from);
            match res {
                Ok(count) => {
                    if count > 0 {
                        info!("Jobs to enqueue: {:?}", count);
                    }
                }
                Err(e) => {
                    error!("Unable to Enqueue jobs, Error: {:?}", e);
                }
            }
        };
        let fut = actix::fut::wrap_future::<_, Self>(fut);
        ctx.spawn(fut);
    }

    fn finished(&mut self, _: &mut Self::Context) {
        warn!("Scheduler for consumer: {:?} stopped", self.id);
    }
}

/// Actix message implements request redis to fetch jobs
#[derive(Debug)]
pub struct FetchJob;

/// Actix message implements request Redis to kill jobs
#[derive(Message, Debug)]
#[rtype(result = "Result<Option<i8>, Error>")]
pub struct KillJob {
    job_id: String,
    error: String,
}

/// Implementation of Actix Handler for killing jobs
impl<M: 'static + std::marker::Unpin + JobHandler + serde::de::DeserializeOwned + Serialize>
    Handler<KillJob> for Consumer<M>
{
    type Result = ResponseFuture<Result<Option<i8>, Error>>;

    fn handle(&mut self, msg: KillJob, _: &mut Self::Context) -> Self::Result {
        let mut con = self.storage.conn.clone();
        let kill_job = redis::Script::new(include_str!("lua/kill_job.lua"));
        let death_timestamp = Utc::now().timestamp();
        let dead_set = self.queue.dead_jobs_set.to_string();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let inflight_set = format!("{}:{}", &self.queue.inflight_jobs_prefix, &self.id);
        let fut = async move {
            kill_job
                .key(inflight_set)
                .key(dead_set)
                .key(job_data_hash)
                .arg(msg.job_id)
                .arg(death_timestamp)
                .arg(msg.error)
                .invoke_async(&mut con)
                .await
                .map_err(Error::from)
        };
        Box::pin(fut)
    }
}

/// Actix message implements request Redis to reenqueue active jobs
#[derive(Message, Debug)]
#[rtype(result = "Result<Option<bool>, Error>")]
pub struct ReenqueueActive {
    jobs: Vec<String>,
}

/// Implementation of Actix Handler for Reenqueueing active jobs
impl<M: 'static + std::marker::Unpin + JobHandler + serde::de::DeserializeOwned + Serialize>
    Handler<ReenqueueActive> for Consumer<M>
{
    type Result = ResponseFuture<Result<Option<bool>, Error>>;

    fn handle(&mut self, msg: ReenqueueActive, _: &mut Self::Context) -> Self::Result {
        let mut con = self.storage.conn.clone();
        let reenqueue_jobs = redis::Script::new(include_str!("lua/reenqueue_active_jobs.lua"));
        let inflight_set = format!("{}:{}", &self.queue.inflight_jobs_prefix, &self.id);
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let signal_list = self.queue.signal_list.to_string();
        let fut = async move {
            reenqueue_jobs
                .key(inflight_set)
                .key(active_jobs_list)
                .key(signal_list)
                .arg(msg.jobs)
                .invoke_async(&mut con)
                .await
                .map_err(Error::from)
        };
        Box::pin(fut)
    }
}

/// Actix message implements request Redis to reenqueue orphaned jobs
#[derive(Message, Debug)]
#[rtype(result = "Result<Option<i8>, Error>")]
pub struct ReenqueueOrphaned {
    expired_before: i32, //DateTime<Utc>.timestamp()
    count: i32,
}

/// Implementation of Actix Handler for Reenqueueing active jobs
impl<M: 'static + std::marker::Unpin + JobHandler + serde::de::DeserializeOwned + Serialize>
    Handler<ReenqueueOrphaned> for Consumer<M>
{
    type Result = ResponseFuture<Result<Option<i8>, Error>>;

    fn handle(&mut self, msg: ReenqueueOrphaned, _: &mut Self::Context) -> Self::Result {
        let mut con = self.storage.conn.clone();
        let reenqueue_jobs = redis::Script::new(include_str!("lua/reenqueue_orphaned_jobs.lua"));
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let signal_list = self.queue.signal_list.to_string();
        let consumers_set = self.queue.consumers_set.to_string();
        let fut = async move {
            reenqueue_jobs
                .key(consumers_set)
                .key(active_jobs_list)
                .key(signal_list)
                .arg(msg.expired_before)
                .arg(msg.count)
                .invoke_async(&mut con)
                .await
                .map_err(Error::from)
        };
        Box::pin(fut)
    }
}

/// Implementation of Actix Handler fetching jobs.
impl<M: 'static + std::marker::Unpin + JobHandler + serde::de::DeserializeOwned + Serialize>
    StreamHandler<FetchJob> for Consumer<M>
{
    fn handle(&mut self, _msg: FetchJob, ctx: &mut Self::Context) {
        let mut con = self.storage.conn.clone();
        let data = &self.data;
        let data = data.clone();
        let fetch_jobs = redis::Script::new(include_str!("lua/get_jobs.lua"));
        let consumers_set = self.queue.consumers_set.to_string();
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let inflight_set = format!("{}:{}", self.queue.inflight_jobs_prefix, self.id);
        let signal_list = self.queue.signal_list.to_string();
        let queue_name = self.queue.get_name().clone();
        let addr = ctx.address();
        let fut = async move {
            let res: Result<Vec<Value>, Error> = fetch_jobs
                .key(consumers_set)
                .key(active_jobs_list)
                .key(&inflight_set)
                .key(job_data_hash)
                .key(signal_list)
                .arg("1") // Fetch one Job at a time
                .arg(&inflight_set)
                .invoke_async(&mut con)
                .await
                .map_err(Error::from);
            match res {
                Ok(jobs) => {
                    let job = jobs.get(0);
                    let job = match job {
                        job @ Some(Value::Data(_)) => job.unwrap(),
                        None => {
                            return debug!("No new jobs found");
                        }
                        _ => {
                            return error!(
                                "Decoding Message Failed: {:?}",
                                "unknown result type for next message"
                            )
                        }
                    };

                    match Job::<M>::decode_message(&job) {
                        Err(e) => {
                            error!("Decoding Message Failed: {:?}", e);
                        }
                        Ok(mut job) => {
                            let notify = |err, dur| {
                                warn!(
                                    "Retrying: Queue: [{}] after: {:?} job_id : {:?} |> {:?}",
                                    queue_name, dur, job.id, err
                                );
                            };

                            let start = Instant::now();
                            let res = retry_notify(
                                ExponentialBackoff::default(),
                                || async { Ok(job.handle(&data.as_ref().lock().unwrap()).await?) },
                                notify,
                            )
                            .await;
                            let duration = start.elapsed();
                            info!(
                                "[{}] Time elapsed in handling job is: {:?} with result {:?}",
                                queue_name, duration, res
                            );
                            addr.send(AckJob::from(&job)).await;
                            job.ack();
                        }
                    };
                }
                Err(e) => {
                    debug!("Unable to Fetch jobs, Error: {:?}", e);
                }
            }
        };
        let fut = actix::fut::wrap_future::<_, Self>(fut);
        ctx.spawn(fut);
    }
}

/// Actix message implements request Redis to ack job
#[derive(Message, Debug)]
#[rtype(result = "Result<Option<bool>, Error>")]
pub struct AckJob {
    job_id: String,
}

impl AckJob {
    pub fn from<M>(job: &Job<M>) -> Self {
        AckJob {
            job_id: job.id.to_string(),
        }
    }
}
/// Implementation of Actix Handler for Get message.
impl<M: 'static + std::marker::Unpin + JobHandler + serde::de::DeserializeOwned + Serialize>
    Handler<AckJob> for Consumer<M>
{
    type Result = ResponseFuture<Result<Option<bool>, Error>>;

    fn handle(&mut self, msg: AckJob, _: &mut Self::Context) -> Self::Result {
        let mut con = self.storage.conn.clone();
        let ack_job = redis::Script::new(include_str!("lua/ack_job.lua"));
        let inflight_set = format!("{}:{}", &self.queue.inflight_jobs_prefix, &self.id);
        let data_hash = format!("{}", &self.queue.job_data_hash);
        let fut = async move {
            ack_job
                .key(inflight_set)
                .key(data_hash)
                .arg(msg.job_id)
                .invoke_async(&mut con)
                .await
                .map_err(Error::from)
        };
        Box::pin(fut)
    }
}

/// Actix message implements request Redis to retry jobs
#[derive(Message)]
#[rtype(result = "Result<Option<i8>, Error>")]
pub struct RetryJob<M: JobHandler> {
    job: Job<M>,
    retry_at: DateTime<Utc>,
}

/// Implementation of Actix Handler for retrying jobs
impl<M: 'static + std::marker::Unpin + JobHandler + serde::de::DeserializeOwned + Serialize>
    Handler<RetryJob<M>> for Consumer<M>
{
    type Result = ResponseFuture<Result<Option<i8>, Error>>;

    fn handle(&mut self, msg: RetryJob<M>, _: &mut Self::Context) -> Self::Result {
        let mut con = self.storage.conn.clone();
        let retry_jobs = redis::Script::new(include_str!("lua/retry_job.lua"));
        let inflight_set = format!("{}:{}", &self.queue.inflight_jobs_prefix, &self.id);
        let scheduled_jobs_set = self.queue.scheduled_jobs_set.to_string();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let id = &msg.job.id.clone();
        let id = id.to_string();
        let message = MessageEncodable::encode_message(&msg.job).unwrap();
        let fut = async move {
            retry_jobs
                .key(inflight_set)
                .key(scheduled_jobs_set)
                .key(job_data_hash)
                .arg(id)
                .arg(msg.retry_at.timestamp())
                .arg(message) // This needs to be new job data
                .invoke_async(&mut con)
                .await
                .map_err(Error::from)
        };
        Box::pin(fut)
    }
}

/// Actix message implements request Redis to kill jobs
#[derive(Message, Debug)]
#[rtype(result = "Result<Option<bool>, Error>")]
pub struct RegisterConsumer;

/// Implementation of Actix Handler for killing jobs
impl<M: 'static + std::marker::Unpin + JobHandler + serde::de::DeserializeOwned + Serialize>
    Handler<RegisterConsumer> for Consumer<M>
{
    type Result = ResponseFuture<Result<Option<bool>, Error>>;

    fn handle(&mut self, msg: RegisterConsumer, _: &mut Self::Context) -> Self::Result {
        let mut con = self.storage.conn.clone();
        let register_consumer = redis::Script::new(include_str!("lua/register_consumer.lua"));
        let inflight_set = format!("{}:{}", &self.queue.inflight_jobs_prefix, self.id);
        let consumers_set = self.queue.consumers_set.to_string();
        let timestamp = Utc::now().timestamp();
        let fut = async move {
            register_consumer
                .key(consumers_set)
                .arg(timestamp)
                .arg(inflight_set)
                .invoke_async(&mut con)
                .await
                .map_err(Error::from)
        };
        Box::pin(fut)
    }
}

impl<M: 'static + std::marker::Unpin + JobHandler + serde::de::DeserializeOwned + Serialize>
    Handler<Stop> for Consumer<M>
{
    type Result = ();

    fn handle(&mut self, _: Stop, ctx: &mut Self::Context) -> Self::Result {
        ctx.stop();
    }
}

impl<M: 'static + std::marker::Unpin + JobHandler + serde::de::DeserializeOwned + Serialize> Actor
    for Consumer<M>
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        let queue = self.queue.get_name().clone();
        let consumer_id = self.id.clone();
        let addr = ctx.address();
        let fut = async move {
            let reg = addr.send(RegisterConsumer).await;
            match reg {
                Ok(Ok(Some(true))) => {
                    info!(
                        "Consumer: [{:}] for Queue [{:}] successfully registered",
                        consumer_id, queue
                    );
                }
                _ => {
                    addr.send(Stop).await.unwrap();
                }
            };
        };
        let fut = actix::fut::wrap_future::<_, Self>(fut);
        ctx.spawn(fut);
        // add stream
        let start = Instant::now() + Duration::from_millis(50);
        let heart_beat = interval_at(start, Duration::from_secs(30)).map(|_| HeartBeat);
        Self::add_stream(heart_beat, ctx);
        debug!("Added consumer: {:?} heartbeat", self.id);
        let schedule = interval_at(start, Duration::from_secs(10)).map(|_| Schedule);
        Self::add_stream(schedule, ctx);
        debug!("Added consumer: {:?} scheduler", self.id);
        let fetch = interval_at(start, Duration::from_millis(250)).map(move |_| FetchJob);
        Self::add_stream(fetch, ctx);
        debug!("Added consumer: {:?} fetcher", self.id);
        // let reenqueue = interval_at(start, Duration::from_secs(30)).map(|_| ReenqueueOrphaned {
        //     count: 10,
        //     expired_before: 0
        // });
        // Self::add_stream(reenqueue, ctx);
    }
}

// To use actor with supervisor actor has to implement `Supervised` trait
impl<M: 'static + std::marker::Unpin + JobHandler + serde::de::DeserializeOwned + Serialize>
    actix::Supervised for Consumer<M>
{
    fn restarting(&mut self, _: &mut Context<Consumer<M>>) {
        info!("Restarting Consumer: {:?}", self.id);
    }
}
