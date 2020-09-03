use crate::error::TaskError as Error;
use actix::prelude::*;
use chrono::prelude::*;
use log::{debug, info};
use redis::{aio::MultiplexedConnection, Client, Value};

const ACTIVE_JOBS_LIST: &str = "{queue}:active";
const CONSUMERS_SET: &str = "{queue}:consumers";
const DEAD_JOBS_SET: &str = "{queue}:deadjobs";
const INFLIGHT_JOB_SET: &str = "{queue}:inflight";
const JOB_DATA_HASH: &str = "{queue}:data";
const SCHEDULED_JOBS_SET: &str = "{queue}:scheduled";
const SIGNAL_LIST: &str = "{queue}:signal";

pub struct Queue {
    name: String,
    active_jobs_list: String,
    consumers_set: String,
    dead_jobs_set: String,
    inflight_jobs_prefix: String,
    job_data_hash: String,
    scheduled_jobs_set: String,
    signal_list: String,
}

impl Queue {
    pub fn new(name: &str) -> Self {
        Queue {
            name: name.to_string(),
            active_jobs_list: ACTIVE_JOBS_LIST.replace("{queue}", &name),
            consumers_set: CONSUMERS_SET.replace("{queue}", &name),
            dead_jobs_set: DEAD_JOBS_SET.replace("{queue}", &name),
            inflight_jobs_prefix: INFLIGHT_JOB_SET.replace("{queue}", &name),
            job_data_hash: JOB_DATA_HASH.replace("{queue}", &name),
            scheduled_jobs_set: SCHEDULED_JOBS_SET.replace("{queue}", &name),
            signal_list: SIGNAL_LIST.replace("{queue}", &name),
        }
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }
}

/// Actix Scheduler queue actor.
pub struct QueueActor {
    conn: MultiplexedConnection,
    queue: Queue,
}

impl QueueActor {
    /// Create new Redis queue actor instance.
    pub async fn new(redis_url: &str, queue: Queue) -> Self {
        let client = Client::open(redis_url).unwrap(); // ?
        let (conn, call) = client.get_multiplexed_async_connection().await.unwrap();
        actix::spawn(call);
        QueueActor { conn, queue }
    }
}

/// Implementation actix Actor trait for Redis queue.
impl Actor for QueueActor {
    type Context = Context<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        info!("QueueActor started");
    }
}

/// Actix message implements request Redis to ack job
#[derive(Message, Debug)]
#[rtype(result = "Result<Option<bool>, Error>")]
pub struct AckJob {
    consumer_id: String,
    job_id: String,
}

/// Implementation of Actix Handler for Get message.
impl Handler<AckJob> for QueueActor {
    type Result = ResponseFuture<Result<Option<bool>, Error>>;

    fn handle(&mut self, msg: AckJob, _: &mut Self::Context) -> Self::Result {
        let mut con = self.conn.clone();
        let ack_job = redis::Script::new(include_str!("lua/ack_job.lua"));
        let inflight_set = format!("{}:{}", &self.queue.inflight_jobs_prefix, &msg.consumer_id);
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
pub struct ScheduleJob {
    pub id: String,
    pub time: DateTime<Utc>,
    //interval: RunType
}

/// Implementation of Actix Handler for scheduling a message.
impl Handler<ScheduleJob> for QueueActor {
    type Result = ResponseFuture<Result<JobStatus, Error>>;

    fn handle(&mut self, msg: ScheduleJob, _: &mut Self::Context) -> Self::Result {
        let mut con = self.conn.clone();
        let schedule_job = redis::Script::new(include_str!("lua/schedule_job.lua"));
        let scheduled_jobs_set = self.queue.scheduled_jobs_set.to_string();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let fut = async move {
            schedule_job
                .key(job_data_hash)
                .key(scheduled_jobs_set)
                .arg(&msg.id)
                .arg(&msg.id)
                .arg(msg.time.timestamp())
                .invoke_async(&mut con)
                .await
                .map(|res: i8| {
                    if res > 0 {
                        JobStatus::Success(msg.time.timestamp())
                    } else {
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
    id: String,
    task: Vec<u8>,
}

impl PushJob {
    pub fn create(task: Vec<u8>) -> PushJob {
        let id = format!("{:?}", Utc::now().timestamp_millis());
        PushJob { id, task }
    }
}

/// Implementation of Actix Handler to push job.
impl Handler<PushJob> for QueueActor {
    type Result = ResponseFuture<Result<JobStatus, Error>>;

    fn handle(&mut self, msg: PushJob, _: &mut Self::Context) -> Self::Result {
        let mut con = self.conn.clone();
        let push_job = redis::Script::new(include_str!("lua/push_job.lua"));
        let job_data_hash = self.queue.job_data_hash.to_string();
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let signal_list = self.queue.signal_list.to_string();
        let fut = async move {
            push_job
                .key(job_data_hash)
                .key(active_jobs_list)
                .key(signal_list)
                .arg(&msg.id)
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
                .map_err(Error::from)
        };
        Box::pin(fut)
    }
}

/// Actix message implements request redis to fetch jobs
#[derive(Message, Debug)]
#[rtype(result = "Result<Vec<Value>, Error>")]
pub struct FetchJobs {
    pub count: i32,
    pub consumer_id: String,
}

/// Implementation of Actix Handler fetching jobs.
impl Handler<FetchJobs> for QueueActor {
    type Result = ResponseFuture<Result<Vec<Value>, Error>>;

    fn handle(&mut self, msg: FetchJobs, _: &mut Self::Context) -> Self::Result {
        let mut con = self.conn.clone();
        let fetch_jobs = redis::Script::new(include_str!("lua/get_jobs.lua"));
        let consumers_set = self.queue.consumers_set.to_string();
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let inflight_set = format!("{}:{}", self.queue.inflight_jobs_prefix, &msg.consumer_id);
        let signal_list = self.queue.signal_list.to_string();
        let fut = async move {
            fetch_jobs
                .key(consumers_set)
                .key(active_jobs_list)
                .key(&inflight_set)
                .key(job_data_hash)
                .key(signal_list)
                .arg(msg.count)
                .arg(&inflight_set)
                .invoke_async(&mut con)
                .await
                .map_err(Error::from)
        };
        Box::pin(fut)
    }
}

/// Actix message implements request Redis to Enqueue scheduled jobs
#[derive(Message, Debug)]
#[rtype(result = "Result<i8, Error>")]
pub struct EnqueueJobs(pub i32);

/// Implementation of Actix Handler for enqueue scheduled jobs
impl Handler<EnqueueJobs> for QueueActor {
    type Result = ResponseFuture<Result<i8, Error>>;

    fn handle(&mut self, msg: EnqueueJobs, _: &mut Self::Context) -> Self::Result {
        let mut con = self.conn.clone();
        let enqueue_jobs = redis::Script::new(include_str!("lua/enqueue_scheduled_jobs.lua"));
        let scheduled_jobs_set = self.queue.scheduled_jobs_set.to_string();
        let active_jobs_list = self.queue.active_jobs_list.to_string();
        let signal_list = self.queue.signal_list.to_string();
        let timestamp = Utc::now().timestamp();
        let fut = async move {
            enqueue_jobs
                .key(scheduled_jobs_set)
                .key(active_jobs_list)
                .key(signal_list)
                .arg(timestamp)
                .arg(msg.0)
                .invoke_async(&mut con)
                .await
                .map_err(Error::from)
        };
        Box::pin(fut)
    }
}

/// Actix message implements request Redis to kill jobs
#[derive(Message, Debug)]
#[rtype(result = "Result<Option<i8>, Error>")]
pub struct KillJob {
    job_id: String,
    consumer_id: String,
    error: String,
}

/// Implementation of Actix Handler for killing jobs
impl Handler<KillJob> for QueueActor {
    type Result = ResponseFuture<Result<Option<i8>, Error>>;

    fn handle(&mut self, msg: KillJob, _: &mut Self::Context) -> Self::Result {
        let mut con = self.conn.clone();
        let kill_job = redis::Script::new(include_str!("lua/kill_job.lua"));
        let death_timestamp = Utc::now().timestamp();
        let dead_set = self.queue.dead_jobs_set.to_string();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let inflight_set = format!("{}:{}", &self.queue.inflight_jobs_prefix, &msg.consumer_id);
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
    consumer_id: String,
    jobs: Vec<String>,
}

/// Implementation of Actix Handler for Reenqueueing active jobs
impl Handler<ReenqueueActive> for QueueActor {
    type Result = ResponseFuture<Result<Option<bool>, Error>>;

    fn handle(&mut self, msg: ReenqueueActive, _: &mut Self::Context) -> Self::Result {
        let mut con = self.conn.clone();
        let reenqueue_jobs = redis::Script::new(include_str!("lua/reenqueue_active_jobs.lua"));
        let inflight_set = format!("{}:{}", &self.queue.inflight_jobs_prefix, &msg.consumer_id);
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
impl Handler<ReenqueueOrphaned> for QueueActor {
    type Result = ResponseFuture<Result<Option<i8>, Error>>;

    fn handle(&mut self, msg: ReenqueueOrphaned, _: &mut Self::Context) -> Self::Result {
        let mut con = self.conn.clone();
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

/// Actix message implements request Redis to kill jobs
#[derive(Message, Debug)]
#[rtype(result = "Result<Option<bool>, Error>")]
pub struct RegisterConsumer(pub String);

/// Implementation of Actix Handler for killing jobs
impl Handler<RegisterConsumer> for QueueActor {
    type Result = ResponseFuture<Result<Option<bool>, Error>>;

    fn handle(&mut self, msg: RegisterConsumer, _: &mut Self::Context) -> Self::Result {
        let mut con = self.conn.clone();
        let register_consumer = redis::Script::new(include_str!("lua/register_consumer.lua"));
        let inflight_set = format!("{}:{}", &self.queue.inflight_jobs_prefix, &msg.0);
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

/// Actix message implements request Redis to retry jobs
#[derive(Message, Debug)]
#[rtype(result = "Result<Option<i8>, Error>")]
pub struct RetryJob {
    job_id: String,
    retry_at: DateTime<Utc>,
    consumer_id: String,
}

/// Implementation of Actix Handler for retrying jobs
impl Handler<RetryJob> for QueueActor {
    type Result = ResponseFuture<Result<Option<i8>, Error>>;

    fn handle(&mut self, msg: RetryJob, _: &mut Self::Context) -> Self::Result {
        let mut con = self.conn.clone();
        let retry_jobs = redis::Script::new(include_str!("lua/retry_job.lua"));
        let inflight_set = format!("{}:{}", &self.queue.inflight_jobs_prefix, &msg.consumer_id);
        let scheduled_jobs_set = self.queue.scheduled_jobs_set.to_string();
        let job_data_hash = self.queue.job_data_hash.to_string();
        let fut = async move {
            retry_jobs
                .key(inflight_set)
                .key(scheduled_jobs_set)
                .key(job_data_hash)
                .arg(&msg.job_id)
                .arg(msg.retry_at.timestamp())
                .arg(&msg.job_id) // This needs to be new job data
                .invoke_async(&mut con)
                .await
                .map_err(Error::from)
        };
        Box::pin(fut)
    }
}

// To use actor with supervisor actor has to implement `Supervised` trait
impl actix::Supervised for QueueActor {
    fn restarting(&mut self, _: &mut Context<QueueActor>) {
        debug!("restarting");
    }
}
