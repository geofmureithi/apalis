use std::{future::Future, time::Duration};



#[cfg(feature = "sleep")]
pub async fn sleep(duration: Duration) {
    let mut interval = async_timer::Interval::platform_new(duration);
    interval.wait().await;
}

#[cfg(feature = "tokio-comp")]
#[derive(Clone, Debug, Default)]
pub struct TokioExecutor;

#[cfg(feature = "tokio-comp")]
impl apalis_core::executor::Executor for TokioExecutor {
    fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        tokio::spawn(future);
    }
}
