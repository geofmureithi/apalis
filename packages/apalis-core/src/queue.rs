use crate::job::Job;
use core::marker::PhantomData;

/// Represents a queue for a job
/// One queue carries one job. Create an enum to handle multiple jobs
#[derive(Clone)]
pub struct Queue<J: Job> {
    name: String,
    job_type: PhantomData<J>,
}

impl<J: Job> Queue<J> {
    pub fn new() -> Self {
        Queue {
            name: J::name().to_string(),
            job_type: PhantomData,
        }
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }
}
