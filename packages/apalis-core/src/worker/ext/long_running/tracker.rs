//! Types related to the [`TaskTracker`] collection.
//!
//! Extracted from the [tokio-util](https://github.com/tokio-rs/tokio/blob/master/tokio-util/src/task/task_tracker.rs) crate and modified to be runtime agnostic

use pin_project_lite::pin_project;
use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

/// A task tracker used for waiting until tasks exit.
pub struct TaskTracker {
    inner: Arc<TaskTrackerInner>,
}

/// Represents a task tracked by a [`TaskTracker`].
#[must_use]
#[derive(Debug)]
pub struct TaskTrackerToken {
    task_tracker: TaskTracker,
}

struct TaskTrackerInner {
    /// Keeps track of the state.
    ///
    /// The lowest bit is whether the task tracker is closed.
    ///
    /// The rest of the bits count the number of tracked tasks.
    state: AtomicUsize,
    /// Used to notify when the last task exits.
    wakers: Mutex<VecDeque<Waker>>,
}

pin_project! {
    /// A future that is tracked as a task by a [`TaskTracker`].
    ///
    /// The associated [`TaskTracker`] cannot complete until this future is dropped.
    ///
    /// This future is returned by [`TaskTracker::track_future`].
    #[must_use = "futures do nothing unless polled"]
    pub struct LongRunningFuture<F> {
        #[pin]
        future: F,
        token: TaskTrackerToken,
    }
}

pin_project! {
    /// A future that completes when the [`TaskTracker`] is empty and closed.
    ///
    /// This future is returned by [`TaskTracker::wait`].
    #[must_use = "futures do nothing unless polled"]
    pub struct TaskTrackerWaitFuture {
        task_tracker: TaskTracker,
        registered: bool,
    }
}

impl TaskTrackerInner {
    #[inline]
    fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
            wakers: Mutex::new(VecDeque::new()),
        }
    }

    #[inline]
    fn is_closed_and_empty(&self) -> bool {
        // If empty and closed bit set, then we are done.
        //
        // The acquire load will synchronize with the release store of any previous call to
        // `set_closed` and `drop_task`.
        self.state.load(Ordering::Acquire) == 1
    }

    #[inline]
    fn set_closed(&self) -> bool {
        // The AcqRel ordering makes the closed bit behave like a `Mutex<bool>` for synchronization
        // purposes. We do this because it makes the return value of `TaskTracker::{close,reopen}`
        // more meaningful for the user.
        let state = self.state.fetch_or(1, Ordering::AcqRel);

        // If there are no tasks, and if it was not already closed:
        if state == 0 {
            self.notify_all();
        }

        (state & 1) == 0
    }

    #[inline]
    fn add_task(&self) {
        self.state.fetch_add(2, Ordering::Relaxed);
    }

    #[inline]
    fn drop_task(&self) {
        let state = self.state.fetch_sub(2, Ordering::Release);

        // If this was the last task and we are closed:
        if state == 3 {
            self.notify_all();
        }
    }

    fn notify_all(&self) {
        // Insert an acquire fence. This matters for `drop_task` but doesn't matter for
        // `set_closed` since it already uses AcqRel.
        //
        // This synchronizes with the release store of any other call to `drop_task`, and with the
        // release store in the call to `set_closed`. That ensures that everything that happened
        // before those other calls to `drop_task` or `set_closed` will be visible after this load,
        // and those things will also be visible to anything woken by the call to wake the wakers.
        self.state.load(Ordering::Acquire);

        if let Ok(mut wakers) = self.wakers.lock() {
            for waker in wakers.drain(..) {
                waker.wake();
            }
        }
    }

    fn register_waker(&self, waker: &Waker) {
        if let Ok(mut wakers) = self.wakers.lock() {
            wakers.push_back(waker.clone());
        }
    }
}

impl TaskTracker {
    /// Creates a new `TaskTracker`.
    ///
    /// The `TaskTracker` will start out as open.
    #[must_use]
    pub(super) fn new() -> Self {
        Self {
            inner: Arc::new(TaskTrackerInner::new()),
        }
    }

    /// Waits until this `TaskTracker` is both closed and empty.
    ///
    /// If the `TaskTracker` is already closed and empty when this method is called, then it
    /// returns immediately.
    ///
    /// The `wait` future is resistant against [ABA problems][aba]. That is, if the `TaskTracker`
    /// becomes both closed and empty for a short amount of time, then it is guarantee that all
    /// `wait` futures that were created before the short time interval will trigger, even if they
    /// are not polled during that short time interval.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    ///
    /// However, the resistance against [ABA problems][aba] is lost when using `wait` as the
    /// condition in a `select!` loop.
    ///
    /// [aba]: https://en.wikipedia.org/wiki/ABA_problem
    #[inline]
    pub fn wait(&self) -> TaskTrackerWaitFuture {
        TaskTrackerWaitFuture {
            task_tracker: self.clone(),
            registered: false,
        }
    }

    /// Close this `TaskTracker`.
    ///
    /// This allows [`wait`] futures to complete. It does not prevent you from spawning new tasks.
    ///
    /// Returns `true` if this closed the `TaskTracker`, or `false` if it was already closed.
    ///
    /// [`wait`]: Self::wait
    #[inline]
    pub fn close(&self) -> bool {
        self.inner.set_closed()
    }

    /// Returns `true` if this `TaskTracker` is [closed](Self::close).
    #[inline]
    #[must_use]
    pub fn is_closed(&self) -> bool {
        (self.inner.state.load(Ordering::Acquire) & 1) != 0
    }

    /// Returns the number of tasks tracked by this `TaskTracker`.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.state.load(Ordering::Acquire) >> 1
    }

    /// Returns `true` if there are no tasks in this `TaskTracker`.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.state.load(Ordering::Acquire) <= 1
    }

    /// Track the provided future.
    #[inline]
    pub(super) fn track_future<F: Future>(&self, future: F) -> LongRunningFuture<F> {
        LongRunningFuture {
            future,
            token: self.token(),
        }
    }

    /// Creates a [`TaskTrackerToken`] representing a task tracked by this `TaskTracker`.
    ///
    /// This token is a lower-level utility than tracking futures directly. Each token is
    /// considered to correspond to a task. As long as the token exists, the `TaskTracker`
    /// cannot complete. Furthermore, the count returned by the [`len`] method will include
    /// the tokens in the count.
    ///
    /// Dropping the token indicates to the `TaskTracker` that the task has exited.
    ///
    /// [`len`]: TaskTracker::len
    #[inline]
    pub(super) fn token(&self) -> TaskTrackerToken {
        self.inner.add_task();
        TaskTrackerToken {
            task_tracker: self.clone(),
        }
    }
}

impl Default for TaskTracker {
    /// Creates a new `TaskTracker`.
    ///
    /// The `TaskTracker` will start out as open.
    #[inline]
    fn default() -> TaskTracker {
        TaskTracker::new()
    }
}

impl Clone for TaskTracker {
    #[inline]
    fn clone(&self) -> TaskTracker {
        Self {
            inner: self.inner.clone(),
        }
    }
}

fn debug_inner(inner: &TaskTrackerInner, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let state = inner.state.load(Ordering::Acquire);
    let is_closed = (state & 1) != 0;
    let len = state >> 1;

    f.debug_struct("TaskTracker")
        .field("len", &len)
        .field("is_closed", &is_closed)
        .field("inner", &(inner as *const TaskTrackerInner))
        .finish()
}

impl fmt::Debug for TaskTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_inner(&self.inner, f)
    }
}

impl TaskTrackerToken {
    /// Returns the [`TaskTracker`] that this token is associated with.
    #[inline]
    #[must_use]
    pub fn task_tracker(&self) -> &TaskTracker {
        &self.task_tracker
    }
}

impl Clone for TaskTrackerToken {
    /// Returns a new `TaskTrackerToken` associated with the same [`TaskTracker`].
    ///
    /// This is equivalent to `token.task_tracker().token()`.
    #[inline]
    fn clone(&self) -> TaskTrackerToken {
        self.task_tracker.token()
    }
}

impl Drop for TaskTrackerToken {
    /// Dropping the token indicates to the [`TaskTracker`] that the task has exited.
    #[inline]
    fn drop(&mut self) {
        self.task_tracker.inner.drop_task();
    }
}

impl<F: Future> Future for LongRunningFuture<F> {
    type Output = F::Output;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        self.project().future.poll(cx)
    }
}

impl<F: fmt::Debug> fmt::Debug for LongRunningFuture<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LongRunningFuture")
            .field("future", &self.future)
            .field("task_tracker", self.token.task_tracker())
            .finish()
    }
}

impl Future for TaskTrackerWaitFuture {
    type Output = ();

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let me = self.project();

        if me.task_tracker.inner.is_closed_and_empty() {
            return Poll::Ready(());
        }

        if !*me.registered {
            me.task_tracker.inner.register_waker(cx.waker());
            *me.registered = true;
        }

        // Check again after registering the waker in case we missed a notification
        if me.task_tracker.inner.is_closed_and_empty() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl fmt::Debug for TaskTrackerWaitFuture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskTrackerWaitFuture")
            .field("task_tracker", &self.task_tracker)
            .field("registered", &self.registered)
            .finish()
    }
}
