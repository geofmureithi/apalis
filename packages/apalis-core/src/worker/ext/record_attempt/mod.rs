use crate::{backend::Backend, worker::builder::WorkerBuilder, request::Request};

pub trait RecordAttempt<Req, Source, Middleware>: Sized {
    fn record_attempts(self) -> WorkerBuilder<Req, Source, Middleware>;
}

impl<Args, P, M, Ctx> RecordAttempt<Request<Args, Ctx>, P, M>
    for WorkerBuilder<Request<Args, Ctx>, P, M>
where
    P: Backend<Request<Args, Ctx>>,
{
    fn record_attempts(self) -> WorkerBuilder<Request<Args, Ctx>, P, M> {
        let this = self;
        WorkerBuilder {
            name: this.name,
            request: this.request,
            layer: this.layer,
            source: this.source,
            shutdown: this.shutdown,
            event_handler: this.event_handler,
        }
    }
}
