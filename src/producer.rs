use crate::consumer::JobHandler;
use crate::error::TaskError as Error;
use crate::message::MessageEncodable;
use crate::queue::Queue;
use crate::Job;
use crate::RedisStorage;
use actix::prelude::*;
use chrono::prelude::*;
use log::{debug, info};
use serde::Deserialize;
use serde::Serialize;
/// Actix Scheduler queue actor.
pub struct Producer {
    conn: RedisStorage,
    queue: Queue,
}

impl Producer {
    pub fn start<S: Into<String>>(redis: &RedisStorage, queue: S) -> Addr<Self> {
        Producer {
            conn: redis.clone(),
            queue: Queue::new(&queue.into()),
        }
        .start()
    }
}

/// Implementation actix Actor trait for Redis queue.
impl Actor for Producer {
    type Context = Context<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        info!("Producer for Queue [{:?}] started", &self.queue.get_name());
    }
}

impl<T: 'static> Handler<T> for Producer
where
    T: Serialize,
    T: actix::Message<Result = ()>,
    T: for<'de> Deserialize<'de>,
    T: JobHandler,
    T: MessageEncodable,
{
    type Result = ();
    fn handle(&mut self, task: T, ctx: &mut Self::Context) -> Self::Result {
        let addr = ctx.address();
        let fut = async move {
            let job = &mut Job::new(task);
            let id = &job.id.clone();
            let message = MessageEncodable::encode_message(job).unwrap();
            let res = addr.send(PushJob::create(*id, message)).await;
            match res {
                Ok(_) => job.ack(),
                Err(_) => job.reject(),
            }
        }
        .into_actor(self);
        ctx.spawn(fut);
    }
}

/// Status of pushing a job.
#[derive(Debug, PartialEq)]
pub enum JobStatus {
    /// Job sucessfully added
    Success(i64), //Timestamp
    /// Job already exists.
    Exists,
}

/// Actix message implements request Redis to schedule job
#[derive(Message, Debug)]
#[rtype(result = "Result<JobStatus, Error>")]
pub struct ScheduleJob<M: JobHandler> {
    pub time: DateTime<Utc>,
    pub message: M,
}

impl<M: 'static + JobHandler + Serialize> ScheduleJob<M> {
    pub fn new(message: M) -> Self {
        ScheduleJob {
            time: Local::now().into(),
            message,
        }
    }

    pub fn in_minutes(mut self, min: i64) -> Self {
        self.time = self.time + chrono::Duration::minutes(min);
        self
    }
    pub fn in_hours(mut self, min: i64) -> Self {
        self.time = self.time + chrono::Duration::hours(min);
        self
    }
}

/// Implementation of Actix Handler for scheduling a message.
impl<M: 'static + JobHandler + Serialize> Handler<ScheduleJob<M>> for Producer {
    type Result = ResponseFuture<Result<JobStatus, Error>>;

    fn handle(&mut self, msg: ScheduleJob<M>, _: &mut Self::Context) -> Self::Result {
        let mut con = self.conn.conn.clone();
        let schedule_job = redis::Script::new(include_str!("lua/schedule_job.lua"));
        let scheduled_jobs_set = self.queue.scheduled_jobs_set.to_string();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let time = &msg.time;
        let timestamp = time.timestamp();
        let fut = async move {
            let mut job = Job::new(msg.message);
            let id = &job.id;
            let id = id.clone();
            let message = MessageEncodable::encode_message(&job).unwrap();
            schedule_job
                .key(job_data_hash)
                .key(scheduled_jobs_set)
                .arg(id.to_string())
                .arg(message)
                .arg(timestamp)
                .invoke_async(&mut con)
                .await
                .map(|res: i8| {
                    if res > 0 {
                        job.ack();
                        JobStatus::Success(timestamp)
                    } else {
                        job.reject();
                        JobStatus::Exists
                    }
                })
                .map_err(Error::from)
        };
        Box::pin(fut)
    }
}

/// Actix message implements request Redis to push job
#[derive(Message, Debug)]
#[rtype(result = "Result<JobStatus, Error>")]
pub struct PushJob {
    id: uuid::Uuid,
    task: Vec<u8>,
}

impl PushJob {
    pub fn create(id: uuid::Uuid, task: Vec<u8>) -> PushJob {
        PushJob { id, task }
    }
}

/// Implementation of Actix Handler to push job.
impl Handler<PushJob> for Producer {
    type Result = ResponseFuture<Result<JobStatus, Error>>;

    fn handle(&mut self, msg: PushJob, _: &mut Self::Context) -> Self::Result {
        let mut con = self.conn.conn.clone();
        let push_job = redis::Script::new(include_str!("lua/push_job.lua"));
        let job_data_hash = self.queue.job_data_hash.to_string();
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let signal_list = self.queue.signal_list.to_string();
        let fut = async move {
            push_job
                .key(job_data_hash)
                .key(active_jobs_list)
                .key(signal_list)
                .arg(&msg.id.to_string())
                .arg(msg.task)
                .invoke_async(&mut con)
                .await
                .map(|res: i8| {
                    if res > 0 {
                        JobStatus::Success(Utc::now().timestamp())
                    } else {
                        JobStatus::Exists
                    }
                })
                .map_err(|e| {
                    println!("{:?}", e);
                    Error::from(e)
                })
        };
        Box::pin(fut)
    }
}

// To use actor with supervisor actor has to implement `Supervised` trait
impl actix::Supervised for Producer {
    fn restarting(&mut self, _: &mut Context<Producer>) {
        debug!("restarting");
    }
}
