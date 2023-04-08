/// TODO: We could use async-std but this feature is available on unstable only.
/// https://docs.rs/async-std/latest/async_std/stream/fn.interval.html
use super::{Sleep, Timer};
use pin_project_lite::pin_project;

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

/// A Timer that uses the smol runtime.
#[derive(Clone, Debug)]
pub struct AsyncStdTimer;

impl Timer for AsyncStdTimer {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Sleep>> {
        Box::pin(SmolSleep {
            inner: smol::Timer::after(duration),
        })
    }

    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Sleep>> {
        Box::pin(SmolSleep {
            inner: smol::Timer::at(deadline),
        })
    }
}

struct SmolTimeout<T: Future> {
    inner: Pin<Box<smol_timeout::Timeout<T>>>,
}

impl<T> Future for SmolTimeout<T>
where
    T: Future,
{
    type Output = Result<T::Output, smol::io::ErrorKind>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner
            .as_mut()
            .poll(context)
            .map(|res| res.ok_or(smol::io::ErrorKind::TimedOut))
    }
}

pin_project! {
    pub(crate) struct SmolSleep {
        #[pin]
        pub(crate) inner: smol::Timer,
    }
}

impl Future for SmolSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx).map(|_| ())
    }
}
impl Sleep for SmolSleep {}
