pub mod layers;
pub mod task_id;
pub mod codec;
pub mod attempt;

#[cfg(feature = "sleep")]
pub async fn sleep(duration: std::time::Duration) {
    let mut interval = async_timer::Interval::platform_new(duration);
    interval.wait().await;
}

#[cfg(feature = "tokio-comp")]
#[derive(Clone, Debug, Default)]
pub struct TokioExecutor;

#[cfg(feature = "tokio-comp")]
impl apalis_core::executor::Executor for TokioExecutor {
    fn spawn(&self, future: impl std::future::Future<Output = ()> + Send + 'static) {
        tokio::spawn(future);
    }
}
