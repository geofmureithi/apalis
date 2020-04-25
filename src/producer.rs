use crate::actor::{PushJob, QueueActor};
use crate::message::MessageEncodable;
use actix::Addr;

pub struct Producer {
    addr: Addr<QueueActor>,
}

impl Producer {
    pub fn new(addr: Addr<QueueActor>) -> Self {
        Producer { addr }
    }

    pub async fn push_job<J>(&self, job: J)
    where
        J: MessageEncodable,
    {
        let queue = self.addr.clone();
        let message = MessageEncodable::encode_message(&job).unwrap();
        let _res = queue.send(PushJob::create(message)).await.unwrap();
    }
}
