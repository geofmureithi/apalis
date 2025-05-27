use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tower::{
    layer::util::{Identity, Stack},
    Layer, Service, ServiceBuilder, ServiceExt,
};

use crate::{
    backend::{Decoder, Encoder},
    request::Request,
};

type BoxedService<Input, Output> = tower::util::BoxService<Input, Output, crate::error::Error>;

type SteppedService<Compact, Ctx> = BoxedService<Request<StepRequest<Compact>, Ctx>, GoTo<Compact>>;

/// Allows control of the next step
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum GoTo<N = ()> {
    /// Go to the next step immediately
    Next(N),
    /// Delay the next step for some time
    Delay {
        /// The input of the next step
        next: N,
        /// The period to delay
        delay: Duration,
    },
    /// Complete execution
    Done(N),
}

// The request type that carries step information
#[derive(Clone, Debug)]
pub struct StepRequest<T> {
    pub step_index: usize,
    pub step: T,
}

impl<T> StepRequest<T> {
    pub fn new(step_index: usize, inner: T) -> Self {
        Self {
            step_index,
            step: inner,
        }
    }
}

#[derive(Clone)]
pub struct StepBuilder<Input, Current, Svc, Codec = ()> {
    steps: HashMap<usize, Svc>,
    input: PhantomData<Input>,
    current: PhantomData<Current>,
    codec: PhantomData<Codec>,
}

type Ctx = ();

impl<Input, Compact> StepBuilder<Input, Input, SteppedService<Compact, Ctx>> {
    fn new() -> StepBuilder<Input, Input, SteppedService<Compact, Ctx>> {
        StepBuilder {
            steps: HashMap::new(),
            input: PhantomData,
            current: PhantomData,
            codec: PhantomData,
        }
    }
}

impl<Input, Current, Compact, Codec>
    StepBuilder<Input, Current, SteppedService<Compact, Ctx>, Codec>
{
    pub fn step<S, Next>(
        mut self,
        service: S,
    ) -> StepBuilder<Input, Next, SteppedService<Compact, Ctx>, Codec>
    where
        S: Service<Request<Current, Ctx>, Response = GoTo<Next>, Error = crate::error::Error>
            + Send
            + 'static,
        S::Future: Send + 'static,
        Codec: Encoder<Next, Compact = Compact> + Decoder<Current, Compact = Compact>,
        <Codec as Decoder<Current>>::Error: Debug,
        <Codec as Encoder<Next>>::Error: Debug,
    {
        let svc = ServiceBuilder::new()
            .map_request(|req: Request<StepRequest<Compact>, Ctx>| {
                Request::new_with_parts(
                    Codec::decode(&req.args.step).expect(&format!(
                        "Could not decode step, expecting {}",
                        std::any::type_name::<Current>()
                    )),
                    req.parts,
                )
            })
            .map_response(|res| match &res {
                GoTo::Next(next) => {
                    GoTo::Next(Codec::encode(next).expect("Could not encode the next step"))
                }
                GoTo::Delay { next, delay } => GoTo::Delay {
                    next: Codec::encode(next).expect("Could not encode the next step"),
                    delay: *delay,
                },
                GoTo::Done(res) => {
                    GoTo::Done(Codec::encode(res).expect("Could not encode the next step"))
                }
            })
            .service(service);
        self.steps.insert(self.steps.len(), BoxedService::new(svc));
        StepBuilder {
            steps: self.steps,
            input: self.input,
            current: PhantomData,
            codec: self.codec,
        }
    }
}

#[derive(Clone)]
pub struct StepService<S> {
    step_service: S,
    step_index: usize,
}

impl<S, T> Service<StepRequest<T>> for StepService<S>
where
    S: Service<T>,
    S::Future: 'static + Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.step_service.poll_ready(cx)
    }

    fn call(&mut self, req: StepRequest<T>) -> Self::Future {
        if req.step_index == self.step_index {
            // Execute this step
            let future = self.step_service.call(req.step);
            Box::pin(future)
        } else {
            // Skip this step - you might want to return a default response
            // or propagate the request somehow. This is a simplified version.
            let step_index = self.step_index;
            Box::pin(async move {
                // In a real implementation, you'd probably want to handle this differently
                // Maybe return an error or a default response
                panic!(
                    "Step {} not executed, current step is {}",
                    step_index, req.step_index
                )
            })
        }
    }
}

// A layer that wraps a service to only execute on a specific step
#[derive(Clone)]
pub struct StepLayer<S> {
    step_index: usize,
    step_service: S,
}

impl<S: Clone, D> Layer<D> for StepLayer<S> {
    type Service = StepService<S>;

    fn layer(&self, _inner: D) -> Self::Service {
        StepService {
            step_index: self.step_index,
            step_service: self.step_service.clone(),
        }
    }
}

// trait Counter {
//     const VALUE: usize;
// }

// struct Zero;
// struct Succ<N: Counter>(std::marker::PhantomData<N>);

// impl Counter for Zero {
//     const VALUE: usize = 0;
// }

// impl<N: Counter> Counter for Succ<N> {
//     const VALUE: usize = N::VALUE + 1;
// }

#[cfg(test)]
mod tests {

    use std::io;

    use crate::service_fn::service_fn;

    use super::*;

    #[tokio::test]
    async fn it_works() {
        async fn task1(job: u32) -> Result<GoTo<u64>, io::Error> {
            Ok(GoTo::Next(job as u64))
        }

        async fn task2(_: u64) -> Result<GoTo, io::Error> {
            Ok(GoTo::Next(()))
        }

        async fn task3(_: ()) -> Result<GoTo, io::Error> {
            Ok(GoTo::Done(()))
        }

        struct Compact;

        async fn fallback(_: Compact) -> Result<GoTo, io::Error> {
            Ok(GoTo::Done(()))
        }

        let mut stepper = StepBuilder::new()
            .step(service_fn(task1))
            .step(service_fn(task2))
            .step(service_fn(task3));

        // let res = stepper
        //     .call(StepRequest {
        //         step_index: 2,
        //         step: 9,
        //     })
        //     .await
        //     .unwrap();

        // dbg!(res);
    }
}
