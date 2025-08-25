pub trait MetadataExt<T> {
    type Error;
    fn extract(&self) -> Result<T, Self::Error>;
    fn inject(&mut self, value: T) -> Result<(), Self::Error>;
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, task::Poll, time::Duration};

    use crate::task::{metadata::MetadataExt, Task};
    use tower::Service;

    #[derive(Debug, Clone)]
    pub struct ExampleService<S> {
        service: S,
    }
    #[derive(Debug, Clone, Default)]
    pub struct ExampleConfig {
        timeout: Duration,
    }

    pub struct SampleStore;

    impl MetadataExt<ExampleConfig> for SampleStore {
        type Error = Infallible;
        fn extract(&self) -> Result<ExampleConfig, Self::Error> {
            Ok(ExampleConfig {
                timeout: Duration::from_secs(1),
            })
        }
        fn inject(&mut self, value: ExampleConfig) -> Result<(), Self::Error> {
            unreachable!()
        }
    }

    impl<T: serde::de::DeserializeOwned> MetadataExt<T> for SampleStore {
        type Error = Infallible;
        fn extract(&self) -> Result<T, Self::Error> {
            unimplemented!()
        }
        fn inject(&mut self, value: T) -> Result<(), Self::Error> {
            unimplemented!()
        }
    }

    impl<S, Args, Meta, IdType> Service<Task<Args, Meta, IdType>> for ExampleService<S>
    where
        S: Service<Task<Args, Meta, IdType>>,
        Meta: MetadataExt<ExampleConfig>,
    {
        type Response = S::Response;
        type Error = S::Error;
        type Future = S::Future;

        fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.service.poll_ready(cx)
        }

        fn call(&mut self, request: Task<Args, Meta, IdType>) -> Self::Future {
            let _config = request.ctx.metadata.extract().unwrap_or_default();
            // Do something with config
            self.call(request)
        }
    }
}
