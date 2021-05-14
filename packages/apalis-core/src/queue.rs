use crate::job::Job;
use crate::storage::Storage;
use core::marker::PhantomData;

#[derive(Clone)]
pub struct Queue<J: Job, S: Storage> {
    name: String,
    job_type: PhantomData<J>,
    pub storage: S,
}

impl<J: Job, S: Storage + Clone> Queue<J, S> {
    pub fn new(storage: &S) -> Self {
        Queue {
            name: J::name().to_string(),
            job_type: PhantomData,
            storage: storage.clone(),
        }
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_storage(&self) -> &S {
        &self.storage
    }
}
