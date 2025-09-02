//! Task metadata extension trait and implementations
//!
//! The [`MetadataExt`] trait allows injecting and extracting metadata associated with tasks.
//! It includes implementations for common metadata types.
//!
//! ## Overview
//! - `MetadataExt<T>`: A trait for extracting and injecting metadata of type `T`.
//!
//! # Usage
//! Implement the `MetadataExt` trait for your metadata types to enable easy extraction and injection
//! from task contexts. This allows middleware and services to access and modify task metadata in a
//! type-safe manner.
/// Task metadata extension trait and implementations.
/// This trait allows for injecting and extracting metadata associated with tasks.
pub trait MetadataExt<T> {
    /// The error type that can occur during extraction or injection.
    type Error;
    /// Extract metadata of type `T`.
    fn extract(&self) -> Result<T, Self::Error>;
    /// Inject metadata of type `T`.
    fn inject(&mut self, value: T) -> Result<(), Self::Error>;
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, task::Poll, time::Duration};

    use crate::task::{metadata::MetadataExt, Task};
    use tower::Service;

    #[derive(Debug, Clone)]
    struct ExampleService<S> {
        service: S,
    }
    #[derive(Debug, Clone, Default)]
    struct ExampleConfig {
        timeout: Duration,
    }

    struct SampleStore;

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

    #[cfg(feature = "json")]
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
            self.service.call(request)
        }
    }
}
