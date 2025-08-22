//! Utilities for adapting async functions into a [`Service`].
//!
//! The [`service_fn`] helper and the [`ServiceFn`] struct in this module allow you to wrap
//! async functions or closures into a [`Service`] implementation, which can then be
//! used in service middleware pipelines or other components expecting a [`Service`].
//!
//! This is particularly useful when building lightweight, composable services from plain
//! functions, including those with extracted arguments via [`FromRequest`].
//!
//! # Features
//!
//! - Supports functions with up to 16 additional arguments beyond the core request.
//! - Automatically applies argument extraction using the [`FromRequest`] trait.
//! - Converts output to responses using the [`IntoResponse`] trait.
//! - Captures function argument types at compile time via generics for static dispatch.
//!
//! # Example
//!
//! ```rust
//! use apalis_core::request::Data;
//!
//! #[derive(Clone)]
//! struct State;
//!
//! async fn handler(id: u32, state: Data<State>) -> String {
//!     format!("Got id {} with state", id)
//! }
//!
//! ```
//!
//! # How It Works
//!
//! Internally, [`service_fn`] returns a [`ServiceFn`] wrapper that implements [`Service`]
//! for [`Request<Args, Context>`] values. When the service is called, it:
//!
//! 1. Extracts arguments using [`FromRequest`].
//! 2. Calls the function with extracted arguments.
//! 3. Wraps the result using [`IntoResponse`].
//!
//! [`FromRequest`]: crate::service_fn::from_request::FromRequest
//! [`IntoResponse`]: crate::service_fn::into_response::IntoResponse
//! [`service_fn`]: crate::service_fn::service_fn
//! [`ServiceFn`]: crate::service_fn::ServiceFn
//! [`Service`]: tower_service::Service
//! [`Request<Args, Context>`]: crate::request::Request
use crate::backend::Backend;
use crate::error::BoxDynError;
use crate::task::Task;
use crate::service_fn::from_request::FromRequest;
use crate::worker::builder::{WorkerBuilderExt, WorkerServiceBuilder};
use futures_util::future::Map;
use futures_util::FutureExt;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::task::{Context, Poll};
use tower_service::Service;
pub mod from_request;
pub mod into_response;
use crate::service_fn::into_response::IntoResponse;

/// A helper method to build functions
pub fn service_fn<F, Args, Meta, FnArgs>(f: F) -> ServiceFn<F, Args, Meta, FnArgs> {
    ServiceFn {
        f,
        req: PhantomData,
        fn_args: PhantomData,
    }
}

/// An executable service implemented by a closure.
///
/// See [`service_fn`] for more details.
pub struct ServiceFn<F, Args, Meta, FnArgs> {
    f: F,
    req: PhantomData<(Args, Meta)>,
    fn_args: PhantomData<FnArgs>,
}

impl<T: Copy, Args, Meta, FnArgs> Copy for ServiceFn<T, Args, Meta, FnArgs> {}

impl<T: Clone, Args, Meta, FnArgs> Clone for ServiceFn<T, Args, Meta, FnArgs> {
    fn clone(&self) -> Self {
        ServiceFn {
            f: self.f.clone(),
            req: PhantomData,
            fn_args: PhantomData,
        }
    }
}

impl<T, Args, Meta, FnArgs> fmt::Debug for ServiceFn<T, Args, Meta, FnArgs> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceFn")
            .field("f", &std::any::type_name::<T>())
            .field(
                "req",
                &format_args!(
                    "PhantomData<Request<{}, {}>>",
                    std::any::type_name::<Args>(),
                    std::any::type_name::<Meta>()
                ),
            )
            .field(
                "fn_args",
                &format_args!("PhantomData<{}>", std::any::type_name::<FnArgs>()),
            )
            .finish()
    }
}

/// The Future returned from [`ServiceFn`] service.
pub type FnFuture<F, O, R, E> = Map<F, fn(O) -> std::result::Result<R, E>>;

macro_rules! impl_service_fn {
    ($($K:ident),+) => {
        #[allow(unused_parens)]
        impl<T, F, Args: Send + 'static, R, Meta: Send + 'static, IdType: Send + Clone + 'static, $($K),+> Service<Task<Args, Meta, IdType>> for ServiceFn<T, Args, Meta, ($($K),+)>
        where
            T: FnMut(Args, $($K),+) -> F + Send + Clone + 'static,
            F: Future + Send,
            F::Output: IntoResponse<Output = R>,
            $(
                $K: FromRequest<Task<Args, Meta, IdType>> + Send,
                < $K as FromRequest<Task<Args, Meta, IdType>> >::Error: std::error::Error + 'static + Send + Sync,
            )+
        {
            type Response = R;
            type Error = BoxDynError;
            type Future = futures_util::future::BoxFuture<'static, Result<R, BoxDynError>>;

            fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, task: Task<Args, Meta, IdType>) -> Self::Future {
                let mut svc = self.f.clone();
                #[allow(non_snake_case)]
                let fut = async move {
                    let results: Result<($($K),+), BoxDynError> = { Ok(($($K::from_request(&task).await.map_err(|e| Box::new(e) as BoxDynError)?),+)) };
                    match results {
                        Ok(($($K),+)) => {
                            let req = task.args;
                            (svc)(req, $($K),+).map(F::Output::into_response).await
                        }
                        Err(e) => Err(e),
                    }
                };
                fut.boxed()
            }
        }

        #[allow(unused_parens)]
        impl<T, Args, Meta, F, R, B, $($K),+>
            WorkerServiceBuilder<B, ServiceFn<T, Args, Meta, ($($K),+)>, Args, Meta> for T
        where
            B: Backend<Args, Meta>,
            T: FnMut(Args, $($K),+) -> F,
            F: Future,
            F::Output: IntoResponse<Output = R>,
            $(
                $K: FromRequest<Task<Args, Meta, B::IdType>> + Send,
                < $K as FromRequest<Task<Args, Meta, B::IdType>> >::Error: std::error::Error + 'static + Send + Sync,
            )+
        {
            fn build(self, _: &B) -> ServiceFn<T, Args, Meta, ($($K),+)> {
                service_fn(self)
            }
        }
    };
}

impl<T, F, Args, R, Meta, IdType> Service<Task<Args, Meta, IdType>> for ServiceFn<T, Args, Meta, ()>
where
    T: FnMut(Args) -> F,
    F: Future,
    F::Output: IntoResponse<Output = R>,
{
    type Response = R;
    type Error = BoxDynError;
    type Future = FnFuture<F, F::Output, R, BoxDynError>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, task: Task<Args, Meta, IdType>) -> Self::Future {
        let fut = (self.f)(task.args);

        fut.map(F::Output::into_response)
    }
}

impl<T, Args, Meta, F, R, Backend>
    WorkerServiceBuilder<Backend, ServiceFn<T, Args, Meta, ()>, Args, Meta> for T
where
    T: FnMut(Args) -> F,
    F: Future,
    F::Output: IntoResponse<Output = R>,
{
    fn build(self, _: &Backend) -> ServiceFn<T, Args, Meta, ()> {
        service_fn(self)
    }
}

impl<Args, Meta, S, B> WorkerServiceBuilder<B, S, Args, Meta> for S
where
    S: Service<Task<Args, Meta, B::IdType>>,
    B: Backend<Args, Meta>
{
    fn build(self, _: &B) -> S {
        self
    }
}

impl_service_fn!(A);
impl_service_fn!(A1, A2);
impl_service_fn!(A1, A2, A3);
impl_service_fn!(A1, A2, A3, A4);
impl_service_fn!(A1, A2, A3, A4, A5);
impl_service_fn!(A1, A2, A3, A4, A5, A6);
impl_service_fn!(A1, A2, A3, A4, A5, A6, A7);
impl_service_fn!(A1, A2, A3, A4, A5, A6, A7, A8);
impl_service_fn!(A1, A2, A3, A4, A5, A6, A7, A8, A9);
impl_service_fn!(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10);
impl_service_fn!(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11);
impl_service_fn!(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12);
impl_service_fn!(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13);
impl_service_fn!(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14);
impl_service_fn!(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15);
impl_service_fn!(A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16);
