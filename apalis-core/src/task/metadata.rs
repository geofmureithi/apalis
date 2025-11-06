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
use crate::task::Task;
use crate::task_fn::FromRequest;

/// Metadata wrapper for task contexts.
#[derive(Debug, Clone)]
pub struct Meta<T>(T);
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

impl<T, Args: Send + Sync, Ctx: MetadataExt<T> + Send + Sync, IdType: Send + Sync>
    FromRequest<Task<Args, Ctx, IdType>> for Meta<T>
{
    type Error = Ctx::Error;

    async fn from_request(task: &Task<Args, Ctx, IdType>) -> Result<Self, Self::Error> {
        task.parts.ctx.extract().map(Meta)
    }
}

#[cfg(test)]
#[allow(unused)]
mod tests {
    use std::{convert::Infallible, fmt::Debug, task::Poll, time::Duration};

    use crate::{
        error::BoxDynError,
        task::{
            Task,
            metadata::{Meta, MetadataExt},
        },
        task_fn::FromRequest,
    };
    use futures_core::future::BoxFuture;
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
        fn inject(&mut self, _: ExampleConfig) -> Result<(), Self::Error> {
            unreachable!()
        }
    }

    #[cfg(feature = "json")]
    impl<T: serde::de::DeserializeOwned> MetadataExt<T> for SampleStore {
        type Error = Infallible;
        fn extract(&self) -> Result<T, Self::Error> {
            unimplemented!()
        }
        fn inject(&mut self, _: T) -> Result<(), Self::Error> {
            unimplemented!()
        }
    }

    impl<S, Args: Send + Sync + 'static, Ctx: Send + Sync + 'static, IdType: Send + Sync + 'static>
        Service<Task<Args, Ctx, IdType>> for ExampleService<S>
    where
        S: Service<Task<Args, Ctx, IdType>> + Clone + Send + 'static,
        Ctx: MetadataExt<ExampleConfig> + Send,
        Ctx::Error: Debug,
        S::Future: Send + 'static,
    {
        type Response = S::Response;
        type Error = S::Error;
        type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.service.poll_ready(cx)
        }

        fn call(&mut self, request: Task<Args, Ctx, IdType>) -> Self::Future {
            let mut svc = self.service.clone();

            // Do something with config
            Box::pin(async move {
                let _config: Meta<ExampleConfig> = request.extract().await.unwrap();
                svc.call(request).await
            })
        }
    }
}
