use actix::{
    clock::{interval_at, Instant},
    Actor, Addr, Arbiter, AsyncContext, Context,
};
use futures::Future;
use serde::{de::DeserializeOwned, Serialize};
use tower::{
    layer::util::{Identity, Stack},
    Layer, Service,
};

use std::{collections::HashMap, fmt::Debug, marker::PhantomData, time::Duration};

use crate::{
    error::JobError,
    queue::{Heartbeat, Queue},
    request::JobRequest,
    response::JobResult,
    service::JobService,
    storage::Storage,
    streams::FetchJobStream,
};

pub struct QueueBuilder<T, S, M> {
    job: PhantomData<T>,
    layer: M,
    storage: S,
    fetch_interval: Duration,
    heartbeats: HashMap<Heartbeat, Duration>,
}

impl<T, S> QueueBuilder<(), S, Identity>
where
    S: Storage<Output = T>,
{
    pub fn new(storage: S) -> QueueBuilder<T, S, Identity> {
        let job: PhantomData<T> = PhantomData;
        QueueBuilder {
            job,
            layer: Identity::new(),
            storage,
            fetch_interval: Duration::from_millis(50),
            heartbeats: HashMap::new(),
        }
    }
}

impl<T, S, M> QueueBuilder<T, S, M> {
    pub fn layer<U>(self, layer: U) -> QueueBuilder<T, S, Stack<U, M>>
    where
        M: Layer<U>,
    {
        QueueBuilder {
            job: self.job,
            storage: self.storage,
            layer: Stack::new(layer, self.layer),
            fetch_interval: self.fetch_interval,
            heartbeats: self.heartbeats,
        }
    }

    // /// Conditionally reject requests based on `predicate`.
    // ///
    // /// `predicate` must implement the [`Predicate`] trait.
    // ///
    // /// This wraps the inner service with an instance of the [`Filter`]
    // /// middleware.
    // ///
    // /// [`Filter`]: crate::filter
    // pub fn filter<P>(
    //     self,
    //     predicate: P,
    // ) -> QueueBuilder<T, Stack<::tower::filter::FilterLayer<P>, M>> {
    //     self.layer(::tower::filter::FilterLayer::new(predicate))
    // }

    // /// Conditionally reject requests based on an asynchronous `predicate`.
    // ///
    // /// `predicate` must implement the [`AsyncPredicate`] trait.
    // ///
    // /// This wraps the inner service with an instance of the [`AsyncFilter`]
    // /// middleware.
    // pub fn filter_async<P>(
    //     self,
    //     predicate: P,
    // ) -> QueueBuilder<T, Stack<::tower::filter::AsyncFilterLayer<P>, M>> {
    //     self.layer(::tower::filter::AsyncFilterLayer::new(predicate))
    // }

    // fn finalize<K>(self, service: K) -> M::Service
    // where
    //     M: Layer<K>,
    // {
    //     self.layer.layer(service)
    // }

    pub fn build(self) -> QueueFactory<T, S, M::Service>
    where
        M: Layer<JobService>,
    {
        QueueFactory {
            job: PhantomData,
            service: self.layer.layer(JobService),
            storage: self.storage,
            fetch_interval: self.fetch_interval,
            heartbeats: self.heartbeats,
        }
    }

    pub fn build_fn<F>(self, f: F) -> QueueFactory<T, S, M::Service>
    where
        M: Layer<::tower::util::ServiceFn<F>>,
    {
        QueueFactory {
            job: PhantomData,
            service: self.layer.layer(::tower::util::service_fn(f)),
            storage: self.storage,
            fetch_interval: self.fetch_interval,
            heartbeats: self.heartbeats,
        }
    }
}

pub struct QueueFactory<T, S, M> {
    job: PhantomData<T>,
    pub service: M,
    pub storage: S,
    fetch_interval: Duration,
    heartbeats: HashMap<Heartbeat, Duration>,
}

impl<T, S, M> QueueFactory<T, S, M> {
    pub fn start<Fut>(self) -> Addr<Queue<T, S, M>>
    where
        S: Storage<Output = T> + Unpin + Send + 'static,
        T: Serialize + Debug + DeserializeOwned + Send + 'static,
        M: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = Fut>
            + Unpin
            + Send
            + 'static,
        Fut: Future<Output = Result<JobResult, JobError>> + 'static,
    {
        let arb = &Arbiter::new();
        Actor::start_in_arbiter(&arb.handle(), |ctx: &mut Context<Queue<T, S, M>>| {
            let start = Instant::now() + Duration::from_millis(5);
            ctx.add_stream(FetchJobStream::new(interval_at(
                start,
                Duration::from_millis(100),
            )));
            Queue::new(self.storage, self.service)
        })
    }

    pub fn start_with<Fut, F>(self, f: F) -> Addr<Queue<T, S, M>>
    where
        S: Storage<Output = T> + Unpin + Send + 'static,
        T: Serialize + Debug + DeserializeOwned + Send + 'static,
        M: Service<JobRequest<T>, Response = JobResult, Error = JobError, Future = Fut>
            + Unpin
            + Send
            + 'static,
        Fut: Future<Output = Result<JobResult, JobError>> + 'static,
        F: FnOnce(Self, &mut Context<Queue<T, S, M>>) -> Queue<T, S, M> + Send + 'static,
    {
        let arb = &Arbiter::new();
        Actor::start_in_arbiter(&arb.handle(), |ctx| f(self, ctx))
    }
}
