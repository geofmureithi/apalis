use crate::Consumer;
use crate::JobHandler;
use actix::prelude::*;
use log::{debug, info};
use serde::Serialize;
use std::sync::mpsc;
use std::{net, thread};

use actix_rt::{net::TcpStream, System};
use actix_server::{Server, ServerBuilder, ServiceFactory};

pub struct WorkerRuntime {
    system: System,
}

impl Worker {
    /// Start new server with server builder
    pub fn start<F>(mut factory: F) -> WorkerRuntime
    where
        F: FnMut(ServerBuilder) -> ServerBuilder + Send + 'static,
    {
        let (tx, rx) = mpsc::channel();

        // run server in separate thread
        thread::spawn(move || {
            let sys = System::new();
            factory(Server::build())
                .workers(1)
                .disable_signals()
                .start();

            tx.send(System::current()).unwrap();
            sys.run()
        });
        let system = rx.recv().unwrap();

        WorkerRuntime { system }
    }
}

impl WorkerRuntime {
    pub fn add_consumer<
        M: 'static
            + JobHandler
            + std::marker::Unpin
            + std::marker::Send
            + serde::de::DeserializeOwned
            + Serialize,
    >(
        self,
        consumer: Consumer<M>,
    ) -> Self {
        Actor::start_in_arbiter(&actix::Arbiter::new(), |_| consumer);
        self
    }

    /// Stop http server
    fn stop(&mut self) {
        self.system.stop();
    }

    pub async fn run(self) {
        actix_rt::signal::ctrl_c().await;
    }
}

impl Drop for WorkerRuntime {
    fn drop(&mut self) {
        self.stop()
    }
}

pub struct Worker;

impl Worker {
    pub fn new() -> Self {
        Worker
    }
}
