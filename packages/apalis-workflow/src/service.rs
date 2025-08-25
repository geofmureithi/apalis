use std::{
    collections::{HashMap, VecDeque},
    marker::PhantomData,
    task::{Context, Poll},
};

use apalis_core::{error::BoxDynError, task::Task};
use futures::future::BoxFuture;
use serde_json::Value;
use tower::Service;

use crate::{GoTo, SteppedService};

pub struct WorkFlowService<Compact, Meta> {
    services: HashMap<usize, SteppedService<Compact, Meta>>,
    not_ready: VecDeque<usize>,
    _phantom: PhantomData<Compact>,
}

impl<Meta> Service<Task<Value, Meta>> for WorkFlowService<Value, Meta> {
    type Response = GoTo<Value>;
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
        let idx = 0;
        let cl = self.services.get_mut(&idx).unwrap();
        self.not_ready.push_back(idx);
        cl.call(req)
    }
}
