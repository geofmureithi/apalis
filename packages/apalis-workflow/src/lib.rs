use std::{collections::HashMap, future::Future, marker::PhantomData, time::Duration};

// use futures::{channel::mpsc::Receiver, stream::BoxStream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tower::{util::BoxService, Service, ServiceBuilder, ServiceExt};

use crate::{
    backend::{
        codec::{json::JsonCodec, Codec},
        TaskSink,
    },
    error::BoxDynError,
    request::{Parts, Request},
    service_fn::{into_response::IntoResponse, service_fn},
    worker::builder::{ServiceFactory, WorkerFactory},
    workflow::stepped::{StepBuilder, StepRequest},
};

// use crate::{backend::Backend, error::BoxDynError, request::Request, worker::context::WorkerContext};

pub mod dag;
pub mod stepped;
// pub mod branch;
// pub mod sink;
// pub mod layer;

/// Allows control of the next flow
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum GoTo<N> {
    /// Go to the next flow immediately
    Next(N),
    /// Delay the next flow for some time
    Delay {
        /// The input of the next flow
        next: N,
        /// The period to delay
        delay: Duration,
    },
    /// Complete execution
    Done(N),
}

pub enum ComplexRequest<Compact> {
    Step(StepRequest<Compact>),
}

pub struct ComplexSink<S, Cdc> {
    inner: S,
    cdc: PhantomData<Cdc>,
}

// impl<Args, S: TaskSink<Args>, Cdc: Codec<Args> + Send + Unpin> TaskSink<Args>
//     for ComplexSink<S, Cdc>
// where
//     <Cdc as Codec<Args>>::Compact: Send,
// {
//     type Codec = Cdc;

//     type Compact = <Cdc as Codec<Args>>::Compact;

//     type Error = S::Error;

//     type Context = S::Context;

//     type Timestamp = S::Timestamp;

//     async fn push_raw_request(
//         &mut self,
//         req: Request<Self::Compact, Self::Context>,
//     ) -> Result<Parts<Self::Context>, Self::Error> {
//         todo!()
//     }
// }

pub enum ComplexService {}

pub struct ComplexBuilder<Resource, Compact, Ctx, Cdc> {
    steps: HashMap<
        String,
        Box<dyn FnOnce(Resource) -> BoxService<Request<ComplexRequest<Compact>, Ctx>, (), BoxDynError>>,
    >,
    codec: PhantomData<Cdc>,
}

// impl<Resource, Compact, Ctx> ComplexBuilder<Resource, Compact, Ctx> {
//     fn then_svc(mut self, svc: BoxService<Request<(), ()>, (), BoxDynError>) -> Self {
//         self.steps.insert("k".to_owned(), Box::new(|r| svc));
//         self
//     }
// }

pub trait ComplexThen<Resource, Factory, Compact, Ctx, Codec> {
    fn then_run(self, service: Factory) -> ComplexBuilder<Resource, Compact, Ctx, Codec>;
}

// impl<S, R, Compact, Ctx, Svc> ComplexThen<S, Svc, R, Compact, Ctx> for ComplexBuilder<R, Compact, Ctx>
// where
//     S: ServiceFactory<R, Svc, Compact, Ctx>
// {
//     fn then(mut self, factory: S) -> ComplexBuilder<R, Compact, Ctx,> {
//         let cb = Box::new(|r| factory.service(r).boxed());
//         self.steps.insert("s".to_owned(), cb);
//         self
//     }
// }



impl<R, Compact, Ctx, Cdc: Send> ComplexBuilder<R, Compact, Ctx, Cdc> {
    fn then<S, Svc>(mut self, factory: S) -> Self
    where
        Svc: Service<Request<Compact, Ctx>, Response = (), Error = BoxDynError> + Send + 'static,
        Svc::Future: Send + 'static,
        S: 'static,
        S: ServiceFactory<R, Svc, Compact, Ctx>,
    {
        let cb = Box::new(|r| factory.service(r).boxed());
        self.steps.insert("k".to_owned(), cb);
        self
    }

    fn then_steps<I: Send, C: Send>(
        mut self,
        factory: StepBuilder<
            I,
            C,
            BoxService<Request<Compact, Ctx>, GoTo<Compact>, BoxDynError>,
            Cdc,
            Compact,
            Ctx,
        >,
    ) -> Self
    where
        R: TaskSink<Request<Compact, Ctx>>,
    {
        let cb = Box::new(move |r| {
            let svc = ServiceBuilder::new().map_request(|r| {

            })
            let final_svc = StepBuilder::service(factory, r);
            svc.service(final_svc).boxed()
                
        });
        self.steps.insert("k".to_owned(), cb);
        self
    }
}

// impl<B, Svc, Args, Ctx> ServiceFactory<B, Svc, Args, Ctx> for ComplexBuilder {
//     fn service(self, resource: B) -> Svc {

//     }
// }

pub trait WorkflowServiceFactory<Req> {
    type Response;
    type Error;
    type Sink;
    type Service: Service<Req, Response = GoTo<Self::Response>, Error = Self::Error>;
    type InitError;
    type Future: Future<Output = Result<Self::Service, Self::InitError>>;
    fn new_service(&self, sink: Self::Sink) -> Self::Future;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::Value;
    use tower::{util::BoxService, ServiceExt};

    use crate::{
        backend::{
            codec::json::JsonCodec,
            memory::{JsonMemory, MemoryStorage},
        },
        error::BoxDynError,
        request::Request,
        service_fn::service_fn,
        workflow::{
            stepped::{StepBuilder, StepRequest},
            ComplexBuilder, ComplexRequest, GoTo,
        },
    };

    const ITEMS: u32 = 10;

    #[tokio::test]
    async fn it_works() {
        let b: ComplexBuilder<JsonMemory<Value>, Value, ()> = ComplexBuilder {
            steps: HashMap::new(),
        };

        async fn next(s: Value) -> Result<(), BoxDynError> {
            Ok(())
        }

        let step = StepBuilder::new().step_fn(|s: ()| async { Ok(GoTo::Done(Value::Null)) });

        let builder = b.then(step);
        // let in_memory = MemoryStorage::new();
        // let mut sink = in_memory.sink();
        // for i in 0..ITEMS {
        //     sink.push(i).await.unwrap();
        // }

        // let worker = WorkerBuilder::new("rango-tango")
        //     .backend(in_memory)
        //     .data(Count::default())
        //     .break_circuit()
        //     .long_running()
        //     .ack_with(MyAcknowledger)
        //     .on_event(|ctx, ev| {
        //         println!("On Event = {:?}", ev);
        //     })
        //     .build(task);
        // worker.run().await.unwrap();
    }
}
