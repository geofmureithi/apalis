pub mod redis;

use std::time::Duration;
// use futures::future::BoxFuture;
pub trait Storage {
    fn push(&self, id: String, job: Vec<u8>);
    fn fetch(&self) -> Vec<u8>;
    fn schedule(&self, id: String, job: Vec<u8>, at: Duration);
    fn ack(&self, id: String);
    fn kill(&self, id: String);
    fn retry(&self, id: String, task: Vec<u8>);
    fn enqueue(&self, count: i8);
}
