use crate::error::TaskError as Error;
use redis::{aio::MultiplexedConnection, Client};

pub trait Storage {}

#[derive(Clone)]
pub struct RedisStorage {
    pub conn: MultiplexedConnection,
}

impl RedisStorage {
    pub async fn new<S: Into<String>>(redis: S) -> Result<Self, Error> {
        let client = Client::open(redis.into())?;
        let conn = client.get_multiplexed_async_connection().await?;
        Ok(RedisStorage { conn })
    }
}


impl Storage for RedisStorage {}