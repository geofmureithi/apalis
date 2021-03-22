use redis::RedisError;

#[derive(Debug)]
pub enum TaskError {
    Redis(RedisError),
    AlreadyInQueue(String),
    InThePast,
    Execution(failure::Error),
    External(String)
}
impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "TaskError!")
    }
}

impl From<RedisError> for TaskError {
    fn from(err: RedisError) -> Self {
        TaskError::Redis(err)
    }
}

impl From<failure::Error> for TaskError {
    fn from(err: failure::Error) -> Self {
        TaskError::Execution(err)
    }
}

impl std::error::Error for TaskError {}
