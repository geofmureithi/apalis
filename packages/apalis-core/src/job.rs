use crate::context::JobContext;
use crate::Consumer;

use actix::prelude::*;
use chrono::prelude::*;
use futures::future::BoxFuture;
use log::{debug, warn};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tokio::sync::{oneshot, oneshot::Sender as OneshotSender};

pub trait Job:
    Serialize + DeserializeOwned + MessageEncodable + MessageDecodable + Send + Unpin
{
    type Result: 'static + Debug;
    fn name() -> &'static str {
        std::any::type_name::<Self>()
    }

    fn retries() -> i64 {
        0
    }
}

pub trait JobHandler<C>
where
    C: Consumer + Actor,
    Self: Job,
{
    type Result: JobResponse<C, Self>;
    fn handle(self, ctx: &mut JobContext<C>) -> <Self as JobHandler<C>>::Result;
}

/// Message objects that can be reconstructed from the data stored in Storage.
///
/// Implemented for all `Deserialize` objects by default by relying on Msgpack
/// decoding.
pub trait MessageDecodable
where
    Self: Sized,
{
    /// Decode the given Redis value into a message
    ///
    /// In the default implementation, the string value is decoded by assuming
    /// it was encoded through the Msgpack encoding.
    fn decode_message(value: &Vec<u8>) -> Result<Self, &'static str>;
}

/// Message objects that can be encoded to a string to be stored in Storage.
///
/// Implemented for all `Serialize` objects by default by encoding with Msgpack.
pub trait MessageEncodable
where
    Self: Sized,
{
    /// Encode the value into a bytes array to be inserted into Storage.
    ///
    /// In the default implementation, the object is encoded with Msgpack.
    fn encode_message(&self) -> Result<Vec<u8>, &'static str>;
}

impl<T> MessageDecodable for T
where
    T: DeserializeOwned,
{
    fn decode_message(value: &Vec<u8>) -> Result<T, &'static str> {
        rmp_serde::decode::from_slice(value).or(Err("failed to decode value with msgpack"))
    }
}

impl<T: Serialize> MessageEncodable for T {
    fn encode_message(&self) -> Result<Vec<u8>, &'static str> {
        rmp_serde::encode::to_vec(self).or(Err("failed to encode value"))
    }
}

#[derive(Debug)]
pub enum Error {
    Failed,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum JobState {
    Unacked,
    Acked,
    Rejected,
}

#[derive(Message, Debug, Serialize, Deserialize)]
#[rtype(result = "Result<JobState, Error>")]
pub struct PushJob {
    pub id: uuid::Uuid,
    pub job: Vec<u8>,
    state: JobState,
    pub retries: i64,
}

impl Drop for PushJob {
    fn drop(&mut self) {
        if self.state() == &JobState::Unacked {
            warn!(
                "Something went wrong: Dropping unacknowledged job [{:?}]",
                &self.id
            );
        }
    }
}

#[derive(Message, Debug, Serialize, Deserialize)]
#[rtype(result = "Result<JobState, Error>")]
pub struct ScheduledJob {
    pub(crate) inner: PushJob,
    pub(crate) time: DateTime<Utc>,
}

impl<J: Job> From<J> for PushJob {
    fn from(job: J) -> Self {
        PushJob::new(uuid::Uuid::new_v4(), job)
    }
}

impl PushJob {
    pub fn new<J: Job>(id: uuid::Uuid, job: J) -> Self {
        let job = J::encode_message(&job).unwrap();
        PushJob {
            id,
            job,
            state: JobState::Unacked,
            retries: 0,
        }
    }

    pub async fn handle<C, J>(&mut self, ctx: &mut JobContext<C>)
    where
        C: Consumer + Actor,
        J: JobHandler<C>,
    {
        let (tx, rx) = oneshot::channel();
        let job = J::decode_message(&self.job).unwrap();
        job.handle(ctx).process(Some(tx));
        match rx.await {
            Ok(value) => {
                self.state = JobState::Acked;
                debug!("Job [{}] completed with value: {:?}", self.id, value);
            }
            Err(err) => {
                self.state = JobState::Rejected;
                warn!("Job [{}] failed with error: {:?}", self.id, err);
            }
        }
    }

    pub fn state(&self) -> &JobState {
        &self.state
    }

    pub fn ack(&mut self) {
        self.state = JobState::Acked;
    }

    pub fn reject(&mut self) {
        self.state = JobState::Rejected;
    }
}

pub trait JobResponse<C: Consumer + Actor, J: Job + JobHandler<C>> {
    fn process(self, tx: Option<OneshotSender<<J as Job>::Result>>);
}

// Helper trait for send one shot message from Option<Sender> type.
// None and error are ignored.
trait JobOneshot<M> {
    fn send(self, msg: M);
}

pub type JobFuture<I> = BoxFuture<'static, I>;

impl<C: Consumer + Actor, J: Job<Result = R> + JobHandler<C>, R: Debug + 'static> JobResponse<C, J>
    for JobFuture<R>
{
    fn process(self, tx: Option<OneshotSender<R>>) {
        // TODO: Handle Err here?
        // println!("Type: {}", std::any::type_name::<R>());
        actix_rt::spawn(async { tx.send(self.await) });
    }
}

impl<C, J, R> JobResponse<C, J> for Option<R>
where
    C: Consumer + Actor,
    J: Job<Result = Option<R>> + JobHandler<C>,
    R: Debug + 'static,
{
    fn process(self, tx: Option<OneshotSender<Option<R>>>) {
        tx.send(self)
    }
}

impl<C, J, R, E> JobResponse<C, J> for Result<R, E>
where
    C: Consumer + Actor,
    J: Job<Result = Result<R, E>> + JobHandler<C>,
    R: Debug + 'static,
    E: Debug + 'static,
{
    fn process(self, tx: Option<OneshotSender<Result<R, E>>>) {
        println!("Response {:?}", self);
        tx.send(self)
    }
}

impl<M> JobOneshot<M> for Option<OneshotSender<M>> {
    fn send(self, msg: M) {
        if let Some(tx) = self {
            let _ = tx.send(msg);
        }
    }
}

macro_rules! SIMPLE_JOB_RESULT {
    ($type:ty) => {
        impl<C, J> JobResponse<C, J> for $type
        where
            C: Consumer + Actor,
            J: Job<Result = $type> + JobHandler<C>,
        {
            fn process(self, tx: Option<OneshotSender<$type>>) {
                tx.send(self)
            }
        }
    };
}

SIMPLE_JOB_RESULT!(());
SIMPLE_JOB_RESULT!(u8);
SIMPLE_JOB_RESULT!(u16);
SIMPLE_JOB_RESULT!(u32);
SIMPLE_JOB_RESULT!(u64);
SIMPLE_JOB_RESULT!(usize);
SIMPLE_JOB_RESULT!(i8);
SIMPLE_JOB_RESULT!(i16);
SIMPLE_JOB_RESULT!(i32);
SIMPLE_JOB_RESULT!(i64);
SIMPLE_JOB_RESULT!(isize);
SIMPLE_JOB_RESULT!(f32);
SIMPLE_JOB_RESULT!(f64);
SIMPLE_JOB_RESULT!(String);
SIMPLE_JOB_RESULT!(bool);
