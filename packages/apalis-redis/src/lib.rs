mod storage;

use actix::{AsyncContext, Context};
use apalis_core::monitor::{Monitor, WorkerEvent, WorkerListener};
use redis::{aio::MultiplexedConnection, Cmd};
pub use storage::RedisStorage;

pub struct RedisPubSubListener {
    conn: MultiplexedConnection,
}

impl RedisPubSubListener {
    pub fn new(conn: MultiplexedConnection) -> Self {
        Self { conn }
    }
}

impl WorkerListener for RedisPubSubListener {
    fn on_event(&self, ctx: &mut Context<Monitor>, worker_id: &String, event: &WorkerEvent) {
        let mut conn = self.conn.clone();
        let message = format!("{}: {:?}", worker_id, event);
        let fut = async move {
            let _res: Result<(), _> = Cmd::publish("apalis::workers", message)
                .query_async(&mut conn)
                .await;
        };
        let fut = actix::fut::wrap_future::<_, Monitor>(fut);
        ctx.spawn(fut);
    }
    fn subscribe(&self, _ctx: &mut Context<Monitor>) {
        // let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        // let mut conn = client.get_connection().unwrap();

        // let fut = async move {
        //     let mut pubsub = conn.as_pubsub();
        //     pubsub.subscribe("apalis::workers").unwrap();

        //     loop {
        //         let msg = pubsub.get_message().unwrap();
        //         let payload: String = msg.get_payload().unwrap();
        //         println!("channel '{}': {}", msg.get_channel_name(), payload);
        //     }
        // };
        // //let fut = actix::fut::wrap_future::<_, Monitor>(fut);
        // actix::spawn(fut);
    }
}
