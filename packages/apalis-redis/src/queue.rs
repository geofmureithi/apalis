use apalis::{Job, Queue};
use crate::RedisStorage;
use std::marker::PhantomData;

const ACTIVE_JOBS_LIST: &str = "{queue}:active";
const CONSUMERS_SET: &str = "{queue}:consumers";
const DEAD_JOBS_SET: &str = "{queue}:deadjobs";
const INFLIGHT_JOB_SET: &str = "{queue}:inflight";
const JOB_DATA_HASH: &str = "{queue}:data";
const SCHEDULED_JOBS_SET: &str = "{queue}:scheduled";
const SIGNAL_LIST: &str = "{queue}:signal";

#[derive(Clone)]
pub struct RedisQueue<J: Job> {
    name: String,
    pub(crate) active_jobs_list: String,
    pub(crate) consumers_set: String,
    pub(crate) dead_jobs_set: String,
    pub(crate) inflight_jobs_prefix: String,
    pub(crate) job_data_hash: String,
    pub(crate) scheduled_jobs_set: String,
    pub(crate) signal_list: String,
    job_type: PhantomData<J>,
    pub(crate) storage: RedisStorage,
}

impl<J: Job> RedisQueue<J> {
    pub fn new(queue: &Queue<J, RedisStorage>) -> Self {
        let name = queue.get_name();
        RedisQueue {
            name: name.to_string(),
            active_jobs_list: ACTIVE_JOBS_LIST.replace("{queue}", &name),
            consumers_set: CONSUMERS_SET.replace("{queue}", &name),
            dead_jobs_set: DEAD_JOBS_SET.replace("{queue}", &name),
            inflight_jobs_prefix: INFLIGHT_JOB_SET.replace("{queue}", &name),
            job_data_hash: JOB_DATA_HASH.replace("{queue}", &name),
            scheduled_jobs_set: SCHEDULED_JOBS_SET.replace("{queue}", &name),
            signal_list: SIGNAL_LIST.replace("{queue}", &name),
            job_type: PhantomData,
            storage: queue.storage.clone()
        }
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }
}
