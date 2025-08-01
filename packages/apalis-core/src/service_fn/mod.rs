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
use crate::error::BoxDynError;
use crate::request::Request;
use crate::service_fn::from_request::FromRequest;
use crate::worker::builder::{ServiceFactory, WorkerFactory};
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
pub fn service_fn<F, Args, Ctx, FnArgs>(f: F) -> ServiceFn<F, Args, Ctx, FnArgs> {
    ServiceFn {
        f,
        req: PhantomData,
        fn_args: PhantomData,
    }
}

/// An executable service implemented by a closure.
///
/// See [`service_fn`] for more details.
pub struct ServiceFn<F, Args, Ctx, FnArgs> {
    f: F,
    req: PhantomData<Request<Args, Ctx>>,
    fn_args: PhantomData<FnArgs>,
}

impl<T: Copy, Args, Ctx, FnArgs> Copy for ServiceFn<T, Args, Ctx, FnArgs> {}

impl<T: Clone, Args, Ctx, FnArgs> Clone for ServiceFn<T, Args, Ctx, FnArgs> {
    fn clone(&self) -> Self {
        ServiceFn {
            f: self.f.clone(),
            req: PhantomData,
            fn_args: PhantomData,
        }
    }
}

impl<T, Args, Ctx, FnArgs> fmt::Debug for ServiceFn<T, Args, Ctx, FnArgs> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceFn")
            .field("f", &std::any::type_name::<T>())
            .field(
                "req",
                &format_args!(
                    "PhantomData<Request<{}, {}>>",
                    std::any::type_name::<Args>(),
                    std::any::type_name::<Ctx>()
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
        impl<T, F, Args: Send + 'static, R, Ctx: Send + 'static, $($K),+> Service<Request<Args, Ctx>> for ServiceFn<T, Args, Ctx, ($($K),+)>
        where
            T: FnMut(Args, $($K),+) -> F + Send + Clone + 'static,
            F: Future + Send,
            F::Output: IntoResponse<Output = R>,
            $(
                $K: FromRequest<Request<Args, Ctx>> + Send,
                < $K as FromRequest<Request<Args, Ctx>> >::Error: std::error::Error + 'static + Send + Sync,
            )+
        {
            type Response = R;
            type Error = BoxDynError;
            type Future = futures_util::future::BoxFuture<'static, Result<R, BoxDynError>>;

            fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, task: Request<Args, Ctx>) -> Self::Future {
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
        impl<T, Args, Ctx, F, R, Sink, $($K),+>
            ServiceFactory<Sink, ServiceFn<T, Args, Ctx, ($($K),+)>, Args, Ctx> for T
        where
            T: FnMut(Args, $($K),+) -> F,
            F: Future,
            F::Output: IntoResponse<Output = R>,
            $(
                $K: FromRequest<Request<Args, Ctx>> + Send,
                < $K as FromRequest<Request<Args, Ctx>> >::Error: std::error::Error + 'static + Send + Sync,
            )+
        {
            fn service(self, _: Sink) -> ServiceFn<T, Args, Ctx, ($($K),+)> {
                service_fn(self)
            }
        }
    };
}

impl<T, F, Args, R, Ctx> Service<Request<Args, Ctx>> for ServiceFn<T, Args, Ctx, ()>
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

    fn call(&mut self, task: Request<Args, Ctx>) -> Self::Future {
        let fut = (self.f)(task.args);

        fut.map(F::Output::into_response)
    }
}

impl<T, Args, Ctx, F, R, Sink> ServiceFactory<Sink, ServiceFn<T, Args, Ctx, ()>, Args, Ctx> for T
where
    T: FnMut(Args) -> F,
    F: Future,
    F::Output: IntoResponse<Output = R>,
{
    fn service(self, _: Sink) -> ServiceFn<T, Args, Ctx, ()> {
        service_fn(self)
    }
}

impl<Args, Ctx, S, Sink> ServiceFactory<Sink, S, Args, Ctx> for S
where
    S: Service<Request<Args, Ctx>>,
{
    fn service(self, _: Sink) -> S {
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
