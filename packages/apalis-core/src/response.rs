use std::{any::Any, error::Error};

use chrono::Duration;
use tokio::sync::{oneshot, oneshot::Sender as OneshotSender};

use crate::{error::JobError, job::JobFuture};

#[derive(Debug, Clone)]
pub enum JobResult {
    Success,
    Retry,
    Kill,
    Reschedule(Duration),
}

pub trait JobResponse {
    fn into_response(self, tx: Option<OneshotSender<Result<JobResult, JobError>>>);
}

impl JobResponse for () {
    fn into_response(self, tx: Option<OneshotSender<Result<JobResult, JobError>>>) {
        tx.send(Ok(JobResult::Success));
    }
}

impl JobResponse for JobFuture<()> {
    fn into_response(self, tx: Option<OneshotSender<Result<JobResult, JobError>>>) {
        actix::spawn(async {
            self.await;
            tx.send(Ok(JobResult::Success))
        });
    }
}

impl<T: Any, E: 'static + Error + Send + Sync> JobResponse for Result<T, E> {
    fn into_response(self, tx: Option<OneshotSender<Result<JobResult, JobError>>>) {
        match self {
            Ok(value) => {
                let value_any = &value as &dyn Any;
                match value_any.downcast_ref::<JobResult>() {
                    Some(res) => {
                        tx.send(Ok(res.clone()));
                    }
                    None => {
                        tx.send(Ok(JobResult::Success));
                    }
                }
            }
            Err(e) => tx.send(Err(JobError::Failed(Box::new(e)))),
        }
    }
}

/// Helper trait for send one shot message from Option<Sender> type.
/// None and error are ignored.
trait JobOneshot<M> {
    fn send(self, msg: M);
}

impl<M> JobOneshot<M> for Option<OneshotSender<M>> {
    fn send(self, msg: M) {
        if let Some(tx) = self {
            let _ = tx.send(msg);
        }
    }
}

// impl<J: Job<Result = R>, R: Debug + 'static> IntoJobFuture for JobFuture<R> {
//     fn process(self, tx: Option<OneshotSender<R>>) {
//         // TODO: Handle Err here?
//         // println!("Type: {}", std::any::type_name::<R>());
//         actix_rt::spawn(async { tx.send(self.await) });
//     }
// }

// impl<J, R> JobResponse<J> for Option<R>
// where
//     J: Job<Result = Option<R>>,
//     R: Debug + 'static,
// {
//     fn process(self, tx: Option<OneshotSender<Option<R>>>) {
//         tx.send(self)
//     }
// }

// impl<J, R, E> JobResponse<J> for Result<R, E>
// where
//     J: Job<Result = Result<R, E>>,
//     R: Debug + 'static,
//     E: Debug + 'static,
// {
//     fn process(self, tx: Option<OneshotSender<Result<R, E>>>) {
//         println!("Response {:?}", self);
//         tx.send(self)
//     }
// }

// impl<M> JobOneshot<M> for Option<OneshotSender<M>> {
//     fn send(self, msg: M) {
//         if let Some(tx) = self {
//             let _ = tx.send(msg);
//         }
//     }
// }

// macro_rules! SIMPLE_JOB_RESULT {
//     ($type:ty) => {
//         impl<J> JobResponse<J> for $type
//         where
//             J: Job<Result = $type>,
//         {
//             fn process(self, tx: Option<OneshotSender<$type>>) {
//                 tx.send(self)
//             }
//         }
//     };
// }

// SIMPLE_JOB_RESULT!(());
// SIMPLE_JOB_RESULT!(u8);
// SIMPLE_JOB_RESULT!(u16);
// SIMPLE_JOB_RESULT!(u32);
// SIMPLE_JOB_RESULT!(u64);
// SIMPLE_JOB_RESULT!(usize);
// SIMPLE_JOB_RESULT!(i8);
// SIMPLE_JOB_RESULT!(i16);
// SIMPLE_JOB_RESULT!(i32);
// SIMPLE_JOB_RESULT!(i64);
// SIMPLE_JOB_RESULT!(isize);
// SIMPLE_JOB_RESULT!(f32);
// SIMPLE_JOB_RESULT!(f64);
// SIMPLE_JOB_RESULT!(String);
// SIMPLE_JOB_RESULT!(bool);
