//! Represents the utilities for running workers.
//!
//! This module defines the core `Worker` type used to poll tasks from a backend, execute them using
//! a service, emit lifecycle events, and handle graceful shutdowns. A worker is typically
//! constructed using a [`WorkerBuilder`](crate::builder::WorkerBuilder), and is intended to be run in
//! an asynchronous runtime.
//!
//! # Features
//! - Pluggable backends for task queues (e.g., in-memory, Redis).
//! - Middleware support for request processing.
//! - Stream or future-based worker execution modes.
//! - Built-in event system for logging or metrics.
//! - Job tracking and readiness probing.
//!
//! # Lifecycle
//! A `Worker`:
//! 1. **Starts** by initializing context and heartbeat.
//! 2. **Polls** the backend for new tasks.
//! 3. **Executes** tasks through the provided `tower_service::Service` stack.
//! 4. **Emits events** like `Idle`, `Engage`, `Success`, `Error`, and `HeartBeat`.
//! 5. **Gracefully shuts down** on signal or explicit stop request.
//!
//! # Examples
//! ```rust,no_run
//! use apalis_core::{WorkerBuilder, MemoryStorage};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let storage = MemoryStorage::new();
//!     let sink = storage.sink();
//!     for i in 0..5 {
//!         sink.push(i).await?;
//!     }
//!
//!     async fn handler(task: u32) {
//!         println!("Processing task: {task}");
//!     }
//!
//!     let worker = WorkerBuilder::new("worker-1")
//!         .backend(storage)
//!         .build_fn(handler);
//!
//!     worker.run().await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Streaming events
//! The `stream` interface yields worker events (e.g., `Success`, `Error`) while running:
//! ```rust,no_run
//! let mut stream = worker.stream();
//! while let Some(evt) = stream.next().await {
//!     println!("Event: {:?}", evt);
//! }
//! ```
//!
//! # Test Utilities
//! This module includes the `test_worker` submodule for unit tests and validation of worker behavior.
use crate::backend::Backend;
use crate::error::{BoxDynError, WorkerError};
use crate::monitor::shutdown::Shutdown;
use crate::request::attempt::Attempt;
use crate::request::data::Data;
use crate::request::Request;
use crate::worker::call_all::{CallAllError, CallAllUnordered};
use crate::worker::context::{Tracked, WorkerContext};
use crate::worker::event::Event;
use futures_channel::mpsc::{self, channel, unbounded};
use futures_core::stream::BoxStream;
use futures_util::future::{join, select, BoxFuture, Either};
use futures_util::{Future, FutureExt, Stream, StreamExt};
use pin_project_lite::pin_project;
use std::any::type_name;
use std::fmt::Debug;
use std::fmt::{self, Display};
use std::marker::PhantomData;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll};
use tower_layer::{Identity, Layer, Stack};
use tower_service::Service;

use thiserror::Error;

pub mod builder;
pub mod call_all;
pub mod context;
pub mod event;
pub mod ext;
pub mod state;
pub mod test_worker;

/// A worker that is ready for running
pub struct Worker<Args, Ctx, Backend, Svc, Middleware> {
    pub(crate) name: String,
    pub(crate) backend: Backend,
    pub(crate) service: Svc,
    pub(crate) middleware: Middleware,
    pub(crate) req: PhantomData<Request<Args, Ctx>>,
    pub(crate) shutdown: Option<Shutdown>,
    pub(crate) event_handler: Box<dyn Fn(&WorkerContext, &Event) + Send + Sync>,
}

impl<Args, Ctx, B, Svc, Middleware> fmt::Debug for Worker<Args, Ctx, B, Svc, Middleware>
where
    Svc: fmt::Debug,
    B: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Worker")
            .field("service", &self.service)
            .field("backend", &self.backend)
            .finish()
    }
}

impl<Args, Ctx, B, Svc, M> Worker<Args, Ctx, B, Svc, M> {
    /// Build a worker that is ready for execution
    pub fn new(name: String, backend: B, service: Svc, layers: M) -> Self {
        Worker {
            name,
            backend,
            service,
            middleware: layers,
            req: PhantomData,
            shutdown: None,
            event_handler: Box::new(|_, _| {}),
        }
    }
}

impl<Args, Ctx, S, B, M> Worker<Args, Ctx, B, S, M>
where
    B: Backend<Args, Ctx>,
    S: Service<Request<Args, Ctx>> + Send + 'static,
    B::Stream: Unpin + Send + 'static,
    B::Beat: Unpin + Send + 'static,
    Args: Send + 'static,
    Ctx: Send + 'static,
    B::Error: Into<BoxDynError> + Send + 'static,
    M: Layer<ReadinessService<TrackerService<S>>>,
    B::Layer: Layer<M::Service>,
    <B::Layer as Layer<M::Service>>::Service: Service<Request<Args, Ctx>> + Send + 'static,
    <<B::Layer as Layer<M::Service>>::Service as Service<Request<Args, Ctx>>>::Error:
        Into<BoxDynError> + Send + Sync + 'static,
    <<B::Layer as Layer<M::Service>>::Service as Service<Request<Args, Ctx>>>::Future: Send,
    M::Service: Service<Request<Args, Ctx>> + Send + 'static,
    <<M as Layer<ReadinessService<TrackerService<S>>>>::Service as Service<Request<Args, Ctx>>>::Future: Send,
    <<M as Layer<ReadinessService<TrackerService<S>>>>::Service as Service<Request<Args, Ctx>>>::Error: Into<BoxDynError> + Send + Sync +'static
{
    pub async fn run(self) -> Result<(), WorkerError> {
        let mut ctx = WorkerContext::new::<<B::Layer as Layer<M::Service>>::Service>(&self.name);
        self.run_with_ctx(&mut ctx).await
    }

    pub async fn run_with_ctx(self, ctx: &mut WorkerContext) -> Result<(), WorkerError>{
        let mut stream = self.stream_with_ctx(ctx);
        while let Some(res) = stream.next().await  {
            match res {
                Ok(_) => continue,
                Err(WorkerError::GracefulExit) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    pub fn stream(self) -> impl Stream<Item = Result<Event, WorkerError>>{
        let mut ctx = WorkerContext::new::<M::Service>(&self.name);
        self.stream_with_ctx(&mut ctx)
    }

    pub fn stream_with_ctx(self, ctx: &mut WorkerContext) -> impl Stream<Item = Result<Event, WorkerError>>{
         let backend = self.backend;
        let event_handler = self.event_handler;
        ctx.wrap_listener(event_handler);
        let mut worker = ctx.clone();
        let inner_layers = backend.middleware();
        struct WorkerServiceBuilder<L> {
            layer: L,
        }

        impl<L> WorkerServiceBuilder<L> {
            fn layer<T>(self, layer: T) -> WorkerServiceBuilder<Stack<T, L>> {
                WorkerServiceBuilder {
                    layer: Stack::new(layer, self.layer),
                }
            }
            fn into_inner(self) -> L {
                self.layer
            }
            fn service<S>(&self, service: S) -> L::Service
            where
                L: Layer<S>,
            {
                self.layer.layer(service)
            }
        }
        let svc = WorkerServiceBuilder {
            layer: Data::new(worker.clone())
        };
        let service = svc
            .layer(inner_layers)
            .layer(self.middleware)
            .layer(ReadinessLayer::new(worker.clone()))
            .layer(TrackerLayer::new(worker.clone()))
            .service(self.service);
        let heartbeat = backend.heartbeat(&worker).map(|_| Ok(Event::HeartBeat));

        let stream = backend.poll(&worker);

        let tasks = poll_tasks(service, stream);
        let mut w = worker.clone();
        let mut ww = w.clone();
        let starter : BoxStream<'static, _> = futures_util::stream::once(async move {
            if !ww.is_running() {
                 ww.start()?;
            }
            Ok(None)
        }).filter_map(|res:Result<Option<Event>, WorkerError>| async move {
            match res {
                Ok(_) => None,
                Err(e) => Some(Err(e))
            }
        }).boxed();
        let wait_for_exit: BoxStream<'static, _> = futures_util::stream::once(async move {
            match worker.await {
                Ok(_) => Err(WorkerError::GracefulExit),
                Err(e) => Err(e)
            }
         }).boxed();
        let work_stream = futures_util::stream_select!(wait_for_exit, heartbeat, tasks).map(move |res| {
            if let Ok(e) = &res{
                w.emit(e);
            }
            res
        });
        starter.chain(work_stream)

    }
}

fn poll_tasks<Svc, Stm, Req, Ctx, E: Into<BoxDynError> + Send + 'static>(
    service: Svc,
    stream: Stm,
) -> BoxStream<'static, Result<Event, WorkerError>>
where
    Svc: Service<Request<Req, Ctx>> + Send + 'static,
    Stm: Stream<Item = Result<Option<Request<Req, Ctx>>, E>> + Send + Unpin + 'static,
    Req: Send + 'static,
    Svc::Future: Send,
    Ctx: Send + 'static,
    Svc::Error: Into<BoxDynError> + Sync + Send,
{
    let stream = CallAllUnordered::new(service, stream).map(|r| match r {
        Ok(Some(_)) => Ok(Event::Success),
        Ok(None) => Ok(Event::Idle),
        Err(CallAllError::ServiceError(err)) => Ok(Event::Error(err.into().into())),
        Err(CallAllError::StreamError(err)) => Err(WorkerError::StreamError(err)),
    });
    stream.boxed()
}

#[derive(Debug, Clone)]
struct TrackerLayer {
    ctx: WorkerContext,
}

impl TrackerLayer {
    fn new(ctx: WorkerContext) -> Self {
        Self { ctx }
    }
}

impl<S> Layer<S> for TrackerLayer {
    type Service = TrackerService<S>;

    fn layer(&self, service: S) -> Self::Service {
        TrackerService {
            ctx: self.ctx.clone(),
            service,
        }
    }
}
#[derive(Debug, Clone)]
pub struct TrackerService<S> {
    ctx: WorkerContext,
    service: S,
}

impl<S, Args, Ctx> Service<Request<Args, Ctx>> for TrackerService<S>
where
    S: Service<Request<Args, Ctx>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Tracked<AttemptOnPollFuture<S::Future>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Args, Ctx>) -> Self::Future {
        let attempt = request.parts.attempt.clone();
        self.ctx.track(AttemptOnPollFuture {
            attempt,
            fut: self.service.call(request),
            polled: false,
        })
    }
}

pin_project_lite::pin_project! {
    pub struct AttemptOnPollFuture<Fut> {
        attempt: Attempt,
        #[pin]
        fut: Fut,
        polled: bool,
    }
}

impl<Fut: Future> Future for AttemptOnPollFuture<Fut> {
    type Output = Fut::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if *this.polled == false {
            *this.polled = true;
            this.attempt.increment();
        }
        this.fut.poll_unpin(cx)
    }
}

#[derive(Clone)]
struct ReadinessLayer {
    ctx: WorkerContext,
}

impl ReadinessLayer {
    fn new(ctx: WorkerContext) -> Self {
        Self { ctx }
    }
}

impl<S> Layer<S> for ReadinessLayer {
    type Service = ReadinessService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ReadinessService {
            inner,
            ctx: self.ctx.clone(),
        }
    }
}

pub struct ReadinessService<S> {
    inner: S,
    ctx: WorkerContext,
}

impl<S, Request> Service<Request> for ReadinessService<S>
where
    S: Service<Request>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Delegate poll_ready to the inner service
        let result = self.inner.poll_ready(cx);
        // Update the readiness state based on the result
        match &result {
            Poll::Ready(Ok(_)) => self.ctx.is_ready.store(true, Ordering::SeqCst),
            Poll::Pending | Poll::Ready(Err(_)) => self.ctx.is_ready.store(false, Ordering::SeqCst),
        }

        result
    }

    fn call(&mut self, req: Request) -> Self::Future {
        self.inner.call(req)
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Deref, sync::atomic::AtomicUsize, time::Duration};

    use futures::{channel::mpsc::SendError, future::ready};

    use crate::{
        backend::{memory::MemoryStorage, TaskSink},
        request::Parts,
        service_fn::{self, service_fn, ServiceFn},
        worker::{
            builder::WorkerBuilder,
            ext::{
                ack::{Acknowledge, AcknowledgementExt},
                circuit_breaker::CircuitBreaker,
                event_listener::EventListenerExt,
                long_running::LongRunningExt,
            },
        },
    };

    use super::*;

    const ITEMS: u32 = 10;

    #[tokio::test]
    async fn it_works() {
        let in_memory = MemoryStorage::new();
        let mut sink = in_memory.sink();
        for i in 0..ITEMS {
            sink.push(i).await.unwrap();
        }

        #[derive(Clone, Debug, Default)]
        struct Count(Arc<AtomicUsize>);

        impl Deref for Count {
            type Target = Arc<AtomicUsize>;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        async fn task(
            task: u32,
            count: Data<Count>,
            ctx: WorkerContext,
        ) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            count.fetch_add(1, Ordering::Relaxed);
            if task == ITEMS - 1 {
                ctx.stop().unwrap();
                return Err("Worker stopped!")?;
            }
            Ok(())
        }

        #[derive(Debug, Clone)]
        struct MyAcknowledger;

        impl<Ctx: Debug> Acknowledge<(), Ctx> for MyAcknowledger {
            type Error = SendError;
            type Future = BoxFuture<'static, Result<(), SendError>>;
            fn ack(&mut self, res: &Result<(), BoxDynError>, parts: &Parts<Ctx>) -> Self::Future {
                println!("{res:?}, {parts:?}");
                ready(Ok(())).boxed()
            }
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .data(Count::default())
            .break_circuit()
            .long_running()
            .ack_with(MyAcknowledger)
            .on_event(|ctx, ev| {
                println!("On Event = {:?}", ev);
            })
            .build(task);
        worker.run().await.unwrap();
    }

    #[tokio::test]
    async fn it_streams() {
        let in_memory = MemoryStorage::new();
        let mut sink = in_memory.sink();

        for i in 0..ITEMS {
            sink.push(i).await.unwrap();
        }

        #[derive(Clone, Debug, Default)]
        struct Count(Arc<AtomicUsize>);

        impl Deref for Count {
            type Target = Arc<AtomicUsize>;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        async fn task(task: u32, count: Data<Count>, worker: WorkerContext) {
            tokio::time::sleep(Duration::from_secs(1)).await;
            count.fetch_add(1, Ordering::Relaxed);
            if task == ITEMS - 1 {
                worker.stop().unwrap();
            }
        }
        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .data(Count::default())
            .break_circuit()
            .long_running()
            .on_event(|ctx, ev| {
                println!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
            })
            .build(task);
        let mut event_stream = worker.stream();
        while let Some(Ok(ev)) = event_stream.next().await {
            println!("On Event = {:?}", ev);
        }
    }
}
