use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
    task::{Context, Poll},
};

use apalis_core::{backend, error::BoxDynError, task::Task};
use futures::future::BoxFuture;
use serde_json::Value;
use tower::Service;

use crate::{CompositeService, GoTo, StepContext, SteppedService};

pub struct WorkFlowService<Compact, Meta, Backend> {
    services: HashMap<usize, CompositeService<Compact, Meta, Backend>>,
    not_ready: VecDeque<usize>,
    _phantom: PhantomData<Compact>,
    backend: Backend,
}
impl<Compact, Meta, Backend> WorkFlowService<Compact, Meta, Backend> {
    pub(crate) fn new(
        services: HashMap<usize, CompositeService<Compact, Meta, Backend>>,
        backend: Backend,
    ) -> Self {
        Self {
            services,
            not_ready: VecDeque::new(),
            _phantom: PhantomData,
            backend,
        }
    }
}

impl<Meta, Backend: Clone + Send + Sync + 'static> Service<Task<Value, Meta>>
    for WorkFlowService<Value, Meta, Backend>
{
    type Response = Value;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        loop {
            // must wait for *all* services to be ready.
            // this will cause head-of-line blocking unless the underlying services are always ready.
            if self.not_ready.is_empty() {
                return Poll::Ready(Ok(()));
            } else {
                if self
                    .services
                    .get_mut(&self.not_ready[0])
                    .unwrap()
                    .svc
                    .poll_ready(cx)?
                    .is_pending()
                {
                    return Poll::Pending;
                }

                self.not_ready.pop_front();
            }
        }
    }

    fn call(&mut self, req: Task<Value, Meta>) -> Self::Future {
        assert!(
            self.not_ready.is_empty(),
            "Workflow must wait for all services to be ready. Did you forget to call poll_ready()?"
        );
        // idx
        // match step_type {
        //
        // }
        let ctx = StepContext::new(self.backend.clone());
        let idx = 1;
        let cl_1 = self.services.get(&(&idx + 1)).unwrap();
        let next_hook = cl_1.pre_hook.clone();
        let cl = self.services.get_mut(&idx).unwrap();
        let svc = &mut cl.svc;

        self.not_ready.push_back(idx);
        let fut = svc.call(req);
        Box::pin(async move {
            let res = fut.await?;
            next_hook(&ctx, &res).await?;
            Ok(res)
        })
    }
}
