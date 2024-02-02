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
pub struct Shutdown(Arc<ShutdownCtx>);

impl Shutdown {
    pub fn new() -> Shutdown {
        Shutdown(Arc::new(ShutdownCtx::new()))
    }

    pub fn shutdown_after<F: Future>(&self, f: F) -> impl Future<Output = F::Output> {
        let handle = self.clone();
        async move {
            let result = f.await;
            handle.shutdown();
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
struct ShutdownCtx {
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
        // Set the shutdown state to true
        self.state.store(true, Ordering::Relaxed);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }

    fn is_shutting_down(&self) -> bool {
        self.state.load(Ordering::Relaxed)
    }
}

impl Shutdown {
    pub fn is_shutting_down(&self) -> bool {
        self.0.is_shutting_down()
    }

    pub fn shutdown(&self) {
        self.0.shutdown()
    }
}

impl Future for Shutdown {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let ctx = &self.0;
        if ctx.state.load(Ordering::Relaxed) {
            Poll::Ready(())
        } else {
            *ctx.waker.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
