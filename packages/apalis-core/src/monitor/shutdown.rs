use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
};

use futures::Future;

/// A shutdown token that stops execution
#[derive(Clone, Debug)]
pub struct Shutdown {
    inner: Arc<ShutdownCtx>,
}

impl Shutdown {
    /// Create a new shutdown handle
    pub fn new() -> Shutdown {
        Shutdown {
            inner: Arc::new(ShutdownCtx::new()),
        }
    }

    /// Set the future to await before shutting down
    pub fn shutdown_after<F: Future>(&self, f: F) -> impl Future<Output = F::Output> {
        let handle = self.clone();
        async move {
            let result = f.await;
            handle.start_shutdown();
            result
        }
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub(crate) struct ShutdownCtx {
    state: AtomicBool,
    waker: Mutex<Option<Waker>>,
}
impl ShutdownCtx {
    fn new() -> ShutdownCtx {
        Self {
            state: AtomicBool::default(),
            waker: Mutex::default(),
        }
    }
    fn shutdown(&self) {
        self.state.store(true, Ordering::Relaxed);
        self.wake();
    }

    fn is_shutting_down(&self) -> bool {
        self.state.load(Ordering::Relaxed)
    }

    pub(crate) fn wake(&self) {
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

impl Shutdown {
    /// Check if the system is shutting down
    pub fn is_shutting_down(&self) -> bool {
        self.inner.is_shutting_down()
    }

    /// Start the shutdown process
    pub fn start_shutdown(&self) {
        self.inner.shutdown()
    }
}

impl Future for Shutdown {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let ctx = &self.inner;
        if ctx.state.load(Ordering::Relaxed) {
            Poll::Ready(())
        } else {
            *ctx.waker.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
