pub mod attempt;
pub mod codec;
pub mod layers;
pub mod task_id;

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

#[cfg(feature = "async-std-comp")]
#[derive(Clone, Debug, Default)]
pub struct AsyncStdExecutor;

#[cfg(feature = "async-std-comp")]
impl apalis_core::executor::Executor for AsyncStdExecutor {
    fn spawn(&self, future: impl std::future::Future<Output = ()> + Send + 'static) {
        async_std::spawn(future);
    }
}
