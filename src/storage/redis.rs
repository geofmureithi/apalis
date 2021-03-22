use crate::storage::Storage;
use std::time::Duration;
use redis::{aio::MultiplexedConnection, Client};

#[derive(Clone)]
pub struct RedisStorage {
    pub client: Client,
}

impl RedisStorage {
    pub fn new<S: Into<String>>(redis: S) -> Self {
        let client = Client::open(redis.into()).unwrap();
        RedisStorage { client }
    }

    pub async fn get_connection(&self) -> MultiplexedConnection {
        let conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        conn
    }
}

impl Storage for RedisStorage {
    fn push(&self, _: String, _: Vec<u8>) {
        todo!()
    }
    fn fetch(&self) -> Vec<u8> {
        todo!()
    }
    fn schedule(&self, _: String, _: Vec<u8>, _: Duration) {
        todo!()
    }
    fn ack(&self, _: String) {
        todo!()
    }
    fn kill(&self, _: String) {
        todo!()
    }
    fn retry(&self, _: String, _: Vec<u8>) {
        todo!()
    }
    fn enqueue(&self, _: i8) {
        todo!()
    }
}
