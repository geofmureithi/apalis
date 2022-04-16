use redis::{aio::MultiplexedConnection, Client, RedisError};

#[derive(Clone)]
pub struct RedisStorage {
    client: Client,
}

impl RedisStorage {
    pub fn new<S: Into<String>>(redis: S) -> Result<Self, RedisError> {
        let client = Client::open(redis.into())?;
        Ok(RedisStorage { client })
    }

    pub async fn get_connection(&self) -> Result<MultiplexedConnection, RedisError> {
        self.client.get_multiplexed_async_connection().await
    }
}
