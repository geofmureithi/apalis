use crate::error::Error;
use crate::layers::extensions::Data;
use crate::request::Request;
use crate::response::IntoResponse;
use futures::future::Map;
use futures::FutureExt;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::task::{Context, Poll};
use tower::Service;

/// A helper method to build functions
pub fn service_fn<T, Req, Ctx, FnArgs>(f: T) -> ServiceFn<T, Req, Ctx, FnArgs> {
    ServiceFn {
        f,
        req: PhantomData,
        fn_args: PhantomData,
    }
}

/// An executable service implemented by a closure.
///
/// See [`service_fn`] for more details.
#[derive(Copy, Clone)]
pub struct ServiceFn<T, Req, Ctx, FnArgs> {
    f: T,
    req: PhantomData<Request<Req, Ctx>>,
    fn_args: PhantomData<FnArgs>,
}

impl<T, Req, Ctx, FnArgs> fmt::Debug for ServiceFn<T, Req, Ctx, FnArgs> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceFn")
            .field("f", &format_args!("{}", std::any::type_name::<T>()))
            .finish()
    }
}

/// The Future returned from [`ServiceFn`] service.
pub type FnFuture<F, O, R, E> = Map<F, fn(O) -> std::result::Result<R, E>>;

/// Handles extraction
pub trait FromRequest<Req>: Sized {
    /// Perform the extraction.
    fn from_request(req: &Req) -> Result<Self, Error>;
}

impl<T: Clone + Send + Sync + 'static, Req, Ctx> FromRequest<Request<Req, Ctx>> for Data<T> {
    fn from_request(req: &Request<Req, Ctx>) -> Result<Self, Error> {
        req.parts.data.get_checked().cloned().map(Data::new)
    }
}

macro_rules! impl_service_fn {
    ($($K:ident),+) => {
        #[allow(unused_parens)]
        impl<T, F, Req, E, R, Ctx, $($K),+> Service<Request<Req, Ctx>> for ServiceFn<T, Req, Ctx, ($($K),+)>
        where
            T: FnMut(Req, $($K),+) -> F,
            F: Future,
            F::Output: IntoResponse<Result = std::result::Result<R, E>>,
            $($K: FromRequest<Request<Req, Ctx>>),+,
            E: From<Error>
        {
            type Response = R;
            type Error = E;
            type Future = futures::future::Either<futures::future::Ready<Result<R, E>>, FnFuture<F, F::Output, R, E>>;

            fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, task: Request<Req, Ctx>) -> Self::Future {

                #[allow(non_snake_case)]
                let fut = {
                    let results: Result<($($K),+), E> = (|| {
                        Ok(($($K::from_request(&task)?),+))
                    })();

                    match results {
                        Ok(($($K),+)) => {
                            let req = task.args;
                            (self.f)(req, $($K),+)
                        }
                        Err(e) => return futures::future::Either::Left(futures::future::err(e).into()),
                    }
                };


                futures::future::Either::Right(fut.map(F::Output::into_response))
            }
        }
    };
}

impl<T, F, Req, E, R, Ctx> Service<Request<Req, Ctx>> for ServiceFn<T, Req, Ctx, ()>
where
    T: FnMut(Req) -> F,
    F: Future,
    F::Output: IntoResponse<Result = std::result::Result<R, E>>,
{
    type Response = R;
    type Error = E;
    type Future = FnFuture<F, F::Output, R, E>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, task: Request<Req, Ctx>) -> Self::Future {
        let fut = (self.f)(task.args);

        fut.map(F::Output::into_response)
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
