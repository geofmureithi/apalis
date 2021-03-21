use crate::Consumer;
use actix::prelude::*;

#[derive(Message)]
#[rtype(result = "Result<(), std::io::Error>")]
pub enum WorkerManagement {
    Status,
    ShutDown,
}

pub struct WorkManager {
    consumers: Vec<Recipient<WorkerManagement>>,
}

impl WorkManager {
    pub fn consumer<
        C: 'static
            + Consumer
            + Send
            + Clone
            + Actor<Context = actix::Context<C>>
            + Handler<WorkerManagement>
            + std::marker::Sync,
    >(
        mut self,
        consumer: C,
    ) -> Self {
        let workers = consumer.workers();
        for _worker in 0..workers {
            let consumer = consumer.clone();
            let addr = Actor::start_in_arbiter(&Arbiter::new().handle(), move |_| consumer);
            self.consumers.push(addr.recipient());
        }
        self
    }
    pub fn new() -> Self {
        WorkManager {
            consumers: Vec::new(),
        }
    }
    /// Start new server with server builder
    pub fn create<F>(mut factory: F) -> Self
    where
        F: FnMut(WorkManager) -> WorkManager + Send + 'static,
    {
        factory(WorkManager::new())
    }

    pub async fn run(self) {
        actix_rt::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl_c");
        let mut addrs = self.consumers.into_iter();
        loop {
            match addrs.next() {
                Some(addr) => {
                    addr.send(WorkerManagement::ShutDown)
                        .await
                        .expect("Unable to Call Shutdown")
                        .unwrap();
                }
                None => break,
            }
        }
    }
}
