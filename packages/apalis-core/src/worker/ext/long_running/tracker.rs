//! Types related to the [`TaskTracker`] collection.
//!
//! Extracted from https://github.com/tokio-rs/tokio/blob/master/tokio-util/src/task/task_tracker.rs

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
///
/// This is usually used together with a cancellation mechanism to implement graceful shutdown.
/// The cancellation mechanism is used to signal to tasks that they should shut down, and the
/// `TaskTracker` is used to wait for them to finish shutting down.
///
/// The `TaskTracker` will also keep track of a `closed` boolean. This is used to handle the case
/// where the `TaskTracker` is empty, but we don't want to shut down yet. This means that the
/// [`wait`] method will wait until *both* of the following happen at the same time:
///
///  * The `TaskTracker` must be closed using the [`close`] method.
///  * The `TaskTracker` must be empty, that is, all tasks that it is tracking must have exited.
///
/// When a call to [`wait`] returns, it is guaranteed that all tracked tasks have exited and that
/// the destructor of the future has finished running.
///
/// # Features
///
/// The `TaskTracker` provides several unique features:
///
///  1. When tasks exit, a `TaskTracker` will allow the task to immediately free its memory.
///  2. By not closing the `TaskTracker`, [`wait`] will be prevented from returning even if
///     the `TaskTracker` is empty.
///  3. A `TaskTracker` does not require mutable access to insert tasks.
///  4. A `TaskTracker` can be cloned to share it with many tasks.
///
/// The first point is important for long-running applications. If you keep inserting tasks and
/// never remove them, their metadata could accumulate and consume memory. With a `TaskTracker`,
/// once tasks exit, they are immediately removed from the `TaskTracker`.
///
/// Note that dropping a `TaskTracker` does not abort the tasks.
///
/// # Examples
///
/// ## Spawn tasks and wait for them to exit
///
/// ```
/// use task_tracker::TaskTracker;
///
/// async fn example() {
///     let tracker = TaskTracker::new();
///
///     // Track some futures
///     for i in 0..10 {
///         let future = async move {
///             println!("Task {} is running!", i);
///         };
///         // You would spawn this with your preferred executor
///         // executor.spawn(tracker.track_future(future));
///     }
///     
///     // Once we spawned everything, we close the tracker.
///     tracker.close();
///
///     // Wait for everything to finish.
///     tracker.wait().await;
///
///     println!("This is printed after all of the tasks.");
/// }
/// ```
///
/// [`close`]: Self::close
/// [`wait`]: Self::wait
pub (super) struct TaskTracker {
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
    pub struct TrackedFuture<F> {
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
    fn set_open(&self) -> bool {
        // See `set_closed` regarding the AcqRel ordering.
        let state = self.state.fetch_and(!1, Ordering::AcqRel);
        (state & 1) == 1
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
    pub(super) fn wait(&self) -> TaskTrackerWaitFuture {
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
    pub(super) fn close(&self) -> bool {
        self.inner.set_closed()
    }

    /// Reopen this `TaskTracker`.
    ///
    /// This prevents [`wait`] futures from completing even if the `TaskTracker` is empty.
    ///
    /// Returns `true` if this reopened the `TaskTracker`, or `false` if it was already open.
    ///
    /// [`wait`]: Self::wait
    #[inline]
    pub(super) fn reopen(&self) -> bool {
        self.inner.set_open()
    }

    /// Returns `true` if this `TaskTracker` is [closed](Self::close).
    #[inline]
    #[must_use]
    pub(super) fn is_closed(&self) -> bool {
        (self.inner.state.load(Ordering::Acquire) & 1) != 0
    }

    /// Returns the number of tasks tracked by this `TaskTracker`.
    #[inline]
    #[must_use]
    pub(super) fn len(&self) -> usize {
        self.inner.state.load(Ordering::Acquire) >> 1
    }

    /// Returns `true` if there are no tasks in this `TaskTracker`.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.state.load(Ordering::Acquire) <= 1
    }

    /// Track the provided future.
    ///
    /// The returned [`TrackedFuture`] will count as a task tracked by this collection, and will
    /// prevent calls to [`wait`] from returning until the task is dropped.
    ///
    /// The task is removed from the collection when it is dropped, not when [`poll`] returns
    /// [`Poll::Ready`].
    ///
    /// # Examples
    ///
    /// Track a future spawned with your executor of choice.
    ///
    /// ```
    /// # async fn my_async_fn() {}
    /// use task_tracker::TaskTracker;
    ///
    /// async fn example() {
    ///     let tracker = TaskTracker::new();
    ///     
    ///     // With async-std
    ///     // async_std::task::spawn(tracker.track_future(my_async_fn()));
    ///     
    ///     // With smol
    ///     // smol::spawn(tracker.track_future(my_async_fn())).detach();
    ///     
    ///     // With any other executor
    ///     // executor.spawn(tracker.track_future(my_async_fn()));
    /// }
    /// ```
    ///
    /// [`Poll::Pending`]: std::task::Poll::Pending
    /// [`poll`]: std::future::Future::poll
    /// [`wait`]: Self::wait
    #[inline]
    pub(super) fn track_future<F: Future>(&self, future: F) -> TrackedFuture<F> {
        TrackedFuture {
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

    /// Returns `true` if both task trackers correspond to the same set of tasks.
    ///
    /// # Examples
    ///
    /// ```
    /// use task_tracker::TaskTracker;
    ///
    /// let tracker_1 = TaskTracker::new();
    /// let tracker_2 = TaskTracker::new();
    /// let tracker_1_clone = tracker_1.clone();
    ///
    /// assert!(TaskTracker::ptr_eq(&tracker_1, &tracker_1_clone));
    /// assert!(!TaskTracker::ptr_eq(&tracker_1, &tracker_2));
    /// ```
    #[inline]
    #[must_use]
    pub(super) fn ptr_eq(left: &TaskTracker, right: &TaskTracker) -> bool {
        Arc::ptr_eq(&left.inner, &right.inner)
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
    /// Returns a new `TaskTracker` that tracks the same set of tasks.
    ///
    /// Since the new `TaskTracker` shares the same set of tasks, changes to one set are visible in
    /// all other clones.
    ///
    /// # Examples
    ///
    /// ```
    /// use task_tracker::TaskTracker;
    ///
    /// async fn example() {
    ///     let tracker = TaskTracker::new();
    ///     let cloned = tracker.clone();
    ///
    ///     // Tokens created on `tracker` are visible in `cloned`.
    ///     let _token = tracker.token();
    ///     assert_eq!(cloned.len(), 1);
    ///
    ///     // Tokens created on `cloned` are visible in `tracker`.
    ///     let _token2 = cloned.token();
    ///     assert_eq!(tracker.len(), 2);
    ///
    ///     // Calling `close` is visible to `cloned`.
    ///     tracker.close();
    ///     assert!(cloned.is_closed());
    ///
    ///     // Calling `reopen` is visible to `tracker`.
    ///     cloned.reopen();
    ///     assert!(!tracker.is_closed());
    /// }
    /// ```
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

impl<F: Future> Future for TrackedFuture<F> {
    type Output = F::Output;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        self.project().future.poll(cx)
    }
}

impl<F: fmt::Debug> fmt::Debug for TrackedFuture<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TrackedFuture")
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
