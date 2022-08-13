use apalis_core::worker::prelude::{WorkerEvent, WorkerListener};
use redis::{aio::MultiplexedConnection, Cmd};

/// A Listener that broadcasts event to Redis via pubsub
pub struct RedisPubSubListener {
    conn: MultiplexedConnection,
}

impl RedisPubSubListener {
    /// New listener from connection
    pub fn new(conn: MultiplexedConnection) -> Self {
        Self { conn }
    }
}

impl WorkerListener for RedisPubSubListener {
    fn on_event(&self, worker_id: &str, event: &WorkerEvent) {
        let mut conn = self.conn.clone();
        let message = format!("{}: {:?}", worker_id, event);
        let fut = async move {
            let _res: Result<(), _> = Cmd::publish("apalis::workers", message)
                .query_async(&mut conn)
                .await;
        };
        tokio::spawn(fut);
    }
}
