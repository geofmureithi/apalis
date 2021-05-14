use crate::Consumer;
use actix::prelude::*;

#[derive(Message)]
#[rtype(result = "()")]
pub enum WorkerManagement {
    Status,
    Stop,
    Restart,
}

pub struct WorkerStatus {
    load: u64,
    running: bool,
    since: chrono::DateTime<chrono::Local>,
    state: String,
}

// pub enum WorkerResponse {
//     Status(WorkerStatus)
//     Action()
// }

pub struct Worker {
    //addrs: Vec<Recipient<WorkerManagement>>,
}

impl Worker {
    pub fn register<F, C>(self, factory: F) -> Self
    where
        F: Fn() -> C,
        C: 'static + Consumer + Send + Actor<Context = actix::Context<C>>, // + Handler<WorkerManagement>,
    {
        let workers = 1;
        self.register_with_threads(workers, factory)
    }

    pub fn register_with_threads<F, C>(mut self, count: usize, factory: F) -> Self
    where
        F: Fn() -> C,
        C: 'static + Consumer + Send + Actor<Context = actix::Context<C>>, // + Handler<WorkerManagement>,
    {
        for _worker in 0..count {
            let consumer = factory();
            let _addr = Actor::start_in_arbiter(&Arbiter::new(), move |_| consumer);
            // self.addrs.push(addr.into());
        }
        self
    }

    pub fn new() -> Self {
        Worker {
            // addrs: vec![]
        }
    }

    pub async fn run(self) {
        actix_rt::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl_c");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestConsumer;
    impl Consumer for TestConsumer {}

    impl Actor for TestConsumer {
        type Context = Context<Self>;
    }

    #[actix::test]
    async fn test_worker() {
        let res = Worker::new().register_with_threads(2, move || TestConsumer);
        assert!(Some(res).is_some())
    }
}
