// use apalis::Error;
// use redis::RedisError;

mod consumer;
mod messages;
mod producer;
mod queue;
mod storage;

pub use consumer::RedisConsumer;
pub use producer::RedisProducer;
pub use storage::RedisStorage;
pub use consumer::RedisJobContext;


// impl From<RedisError> for Error {
//     fn from(_err: RedisError) -> Self {
//         Error::Failed
//     }
// }
