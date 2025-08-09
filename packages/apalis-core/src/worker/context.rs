//! Worker context and task tracking.
//!
//! This module defines [`WorkerContext`], the core structure responsible for managing
//! the execution lifecycle of a worker, tracking tasks, handling shutdown, and emitting
//! lifecycle events. It also provides [`Tracked`] for wrapping and monitoring asynchronous
//! tasks within the worker domain.
//!
//! ## Lifecycle
//! A `WorkerContext` goes through distinct phases of operation:
//!
//! - **Pending**: Created via [`WorkerContext::new`] and must be explicitly started.
//! - **Running**: Activated by calling [`WorkerContext::start`]. The worker becomes ready to accept and track tasks.
//! - **Paused**: Temporarily halted via [`WorkerContext::pause`]. New tasks are blocked from execution.
//! - **Resumed**: Brought back to `Running` using [`WorkerContext::resume`].
//! - **Stopped**: Finalized via [`WorkerContext::stop`]. The worker shuts down gracefully, allowing tracked tasks to complete.
//!
//! The `WorkerContext` itself implements [`Future`], and can be `.await`ed — it resolves
//! once the worker is shut down and all tasks have completed.
//!
//! ## Task Management
//! Asynchronous tasks can be tracked using [`WorkerContext::track`], which wraps a future
//! in a [`Tracked`] type. This ensures:
//! - Task count is incremented before execution and decremented on completion
//! - Shutdown is automatically triggered once all tasks are done
//!
//! Use [`WorkerContext::task_count`] and [`WorkerContext::has_pending_tasks`] to inspect
//! ongoing task state.
//!
//! ## Shutdown Semantics
//! The worker is considered shutting down if:
//! - `stop()` has been called
//! - A shutdown signal (if configured) has been triggered
//!
//! Once shutdown begins, no new tasks should be accepted. Internally, a stored [`Waker`] is
//! used to drive progress toward shutdown completion.
//!
//! ## Event Handling
//! Worker lifecycle events (e.g., `Start`, `Stop`) can be emitted using [`WorkerContext::emit`].
//! Custom handlers can be registered via [`WorkerContext::wrap_listener`] to hook into these transitions.
//!
//! ## Request Integration
//! `WorkerContext` implements [`FromRequest`] so it can be extracted automatically in request
//! handlers when using a compatible framework or service layer.
//!
//! ## Types
//! - [`WorkerContext`] — shared state container for a worker
//! - [`Tracked`] — future wrapper for task lifecycle tracking
use pin_project_lite::pin_project;
use std::{
    any::type_name,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
};

use crate::{
    error::{WorkerError, WorkerStateError},
    monitor::shutdown::Shutdown,
    request::{data::MissingDataError, Request},
    service_fn::from_request::FromRequest,
    worker::{
        event::{CtxEventHandler, Event},
        state::{InnerWorkerState, WorkerState},
    },
};

/// Stores the Workers context
#[derive(Clone)]
pub struct WorkerContext {
    pub(super) name: Arc<String>,
    task_count: Arc<AtomicUsize>,
    waker: Arc<Mutex<Option<Waker>>>,
    state: Arc<WorkerState>,
    pub(crate) shutdown: Option<Shutdown>,
    event_handler: CtxEventHandler,
    pub(super) is_ready: Arc<AtomicBool>,
    pub(super) service: &'static str,
}

impl fmt::Debug for WorkerContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerContext")
            .field("shutdown", &["Shutdown handle"])
            .field("task_count", &self.task_count)
            .field("state", &self.state.load(Ordering::SeqCst))
            .field("service", &self.service)
            .field("is_ready", &self.is_ready)
            .finish()
    }
}

pin_project! {
    /// A future tracked by the worker
    pub struct Tracked<F> {
        ctx: WorkerContext,
        #[pin]
        task: F,
    }
}

impl<F: Future> Future for Tracked<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.project();

        match this.task.poll(cx) {
            res @ Poll::Ready(_) => {
                this.ctx.end_task();
                res
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl WorkerContext {
    pub fn new<S>(name: &str) -> Self {
        Self {
            name: Arc::new(name.to_owned()),
            service: type_name::<S>(),
            task_count: Default::default(),
            waker: Default::default(),
            state: Default::default(),
            shutdown: Default::default(),
            event_handler: Arc::new(Box::new(|_, _| {
                // noop
            })),
            is_ready: Default::default(),
        }
    }

    /// Get the worker id
    pub fn name(&self) -> &String {
        &self.name
    }

    /// Start running the worker
    pub fn start(&mut self) -> Result<(), WorkerError> {
        let current_state = self.state.load(Ordering::SeqCst);
        if current_state != InnerWorkerState::Pending {
            return Err(WorkerError::StateError(WorkerStateError::AlreadyStarted));
        }
        self.state
            .store(InnerWorkerState::Running, Ordering::SeqCst);
        self.is_ready.store(false, Ordering::SeqCst);
        self.emit(&Event::Start);
        info!("Worker {} started", self.name());
        Ok(())
    }

    /// Restart running the worker
    pub(crate) fn restart(&mut self) -> Result<(), WorkerError> {
        self.state
            .store(InnerWorkerState::Pending, Ordering::SeqCst);
        self.is_ready.store(false, Ordering::SeqCst);
        info!("Worker {} restarted", self.name());
        Ok(())
    }

    /// Start a task that is tracked by the worker
    pub fn track<F: Future>(&self, task: F) -> Tracked<F> {
        self.start_task();
        Tracked {
            ctx: self.clone(),
            task,
        }
    }
    /// Pauses a worker, preventing any new jobs from being polled
    pub fn pause(&self) -> Result<(), WorkerError> {
        if !self.is_running() {
            return Err(WorkerError::StateError(WorkerStateError::NotRunning));
        }
        self.state.store(InnerWorkerState::Paused, Ordering::SeqCst);
        info!("Worker {} paused", self.name());
        Ok(())
    }

    /// Resume a worker that is paused
    pub fn resume(&self) -> Result<(), WorkerError> {
        if !self.is_paused() {
            return Err(WorkerError::StateError(WorkerStateError::NotPaused));
        }
        if self.is_shutting_down() {
            return Err(WorkerError::StateError(WorkerStateError::ShuttingDown));
        }
        self.state
            .store(InnerWorkerState::Running, Ordering::SeqCst);
        info!("Worker {} resumed", self.name());
        Ok(())
    }

    /// Calling this function triggers shutting down the worker while waiting for any tasks to complete
    pub fn stop(&self) -> Result<(), WorkerError> {
        let current_state = self.state.load(Ordering::SeqCst);
        if current_state == InnerWorkerState::Pending {
            return Err(WorkerError::StateError(WorkerStateError::NotStarted));
        }
        self.state
            .store(InnerWorkerState::Stopped, Ordering::SeqCst);
        self.wake();
        self.emit_ref(&Event::Stop);
        info!("Worker {} stopped", self.name());
        Ok(())
    }

    /// Returns if the worker is ready to consume new tasks
    pub fn is_ready(&self) -> bool {
        self.is_running() && !self.is_shutting_down() && self.is_ready.load(Ordering::SeqCst)
    }

    /// Get the type of service
    pub fn get_service(&self) -> &str {
        &self.service
    }

    /// Returns whether the worker is running
    pub fn is_running(&self) -> bool {
        self.state.load(Ordering::SeqCst) == InnerWorkerState::Running
    }

    /// Returns whether the worker is pending
    pub fn is_pending(&self) -> bool {
        self.state.load(Ordering::SeqCst) == InnerWorkerState::Pending
    }

    /// Returns whether the worker is paused
    pub fn is_paused(&self) -> bool {
        self.state.load(Ordering::SeqCst) == InnerWorkerState::Paused
    }

    /// Returns the current futures in the worker domain
    /// This include futures spawned via `worker.track`
    pub fn task_count(&self) -> usize {
        self.task_count.load(Ordering::Relaxed)
    }

    /// Returns whether the worker has pending tasks
    pub fn has_pending_tasks(&self) -> bool {
        self.task_count.load(Ordering::Relaxed) > 0
    }

    /// Is the shutdown token called
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown
            .as_ref()
            .map(|s| !self.is_running() || s.is_shutting_down())
            .unwrap_or(!self.is_running())
    }

    /// Allows workers to emit events
    pub fn emit(&mut self, event: &Event) {
        self.emit_ref(event);
    }

    fn emit_ref(&self, event: &Event) {
        let handler = self.event_handler.as_ref();
        handler(self, event);
    }

    pub(crate) fn wrap_listener<F: Fn(&WorkerContext, &Event) + Send + Sync + 'static>(
        &mut self,
        f: F,
    ) {
        let cur = self.event_handler.clone();
        let new: Box<dyn Fn(&WorkerContext, &Event) + Send + Sync + 'static> =
            Box::new(move |ctx, ev| {
                f(&ctx, &ev);
                cur(&ctx, &ev);
            });
        self.event_handler = Arc::new(new);
    }

    fn add_waker(&self, cx: &mut Context<'_>) {
        if let Ok(mut waker_guard) = self.waker.lock() {
            if waker_guard
                .as_ref()
                .map_or(true, |stored_waker| !stored_waker.will_wake(cx.waker()))
            {
                *waker_guard = Some(cx.waker().clone());
            }
        }
    }

    /// Checks if the stored waker matches the current one.
    fn has_recent_waker(&self, cx: &Context<'_>) -> bool {
        if let Ok(waker_guard) = self.waker.lock() {
            if let Some(stored_waker) = &*waker_guard {
                return stored_waker.will_wake(cx.waker());
            }
        }
        false
    }

    fn start_task(&self) {
        self.task_count.fetch_add(1, Ordering::Relaxed);
    }

    fn end_task(&self) {
        if self.task_count.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.wake();
        }
    }

    pub(crate) fn wake(&self) {
        if let Ok(waker) = self.waker.lock() {
            if let Some(waker) = &*waker {
                waker.wake_by_ref();
            }
        }
    }
}

impl Future for WorkerContext {
    type Output = Result<(), WorkerError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let task_count = self.task_count.load(Ordering::Relaxed);
        let state = self.state.load(Ordering::SeqCst);
        if state == InnerWorkerState::Pending {
            return Poll::Ready(Err(WorkerError::StateError(WorkerStateError::NotStarted)));
        }
        if self.is_shutting_down() && task_count == 0 {
            Poll::Ready(Ok(()))
        } else {
            if !self.has_recent_waker(cx) {
                self.add_waker(cx);
            }
            Poll::Pending
        }
    }
}

impl<Req: Sync, Ctx: Sync> FromRequest<Request<Req, Ctx>> for WorkerContext {
    type Error = MissingDataError;
    async fn from_request(req: &Request<Req, Ctx>) -> Result<Self, Self::Error> {
        req.parts.data.get_checked().cloned()
    }
}
