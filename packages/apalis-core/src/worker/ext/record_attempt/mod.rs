use crate::{backend::Backend, request::Request, worker::builder::WorkerBuilder};

pub trait RecordAttempt<Args, Ctx, Source, Middleware>: Sized {
    fn record_attempts(self) -> WorkerBuilder<Args, Ctx, Source, Middleware>;
}

impl<Args, P, M, Ctx> RecordAttempt<Args, Ctx, P, M> for WorkerBuilder<Args, Ctx, P, M>
where
    P: Backend<Args, Ctx>,
{
    fn record_attempts(self) -> WorkerBuilder<Args, Ctx, P, M> {
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
