// /// A trait for acknowledging successful processing
// /// This trait is called even when a task fails.
// /// This is a way of a [`Backend`] to save the result of a job or message
// pub trait Ack<Task, Res, Codec> {
//     /// The data to fetch from context to allow acknowledgement
//     type Context;
//     /// The error returned by the ack
//     type AckError: std::error::Error;

//     /// Acknowledges successful processing of the given request
//     fn ack(
//         &mut self,
//         ctx: &Self::Context,
//         response: &Response<Res, Self::Context>,
//     ) -> impl Future<Output = Result<(), Self::AckError>> + Send;
// }

// impl<
//         T,
//         Res: Clone + Send + Sync + Serialize,
//         Ctx: Clone + Send + Sync,
//         Compact,
//         Cdc: Encoder<Res, Compact = Compact>,
//     > Ack<T, Res, Cdc> for Sender<(Ctx, Response<Res, Compact>)>
// where
//     Cdc::Error: Debug,
//     Compact: Send,
// {
//     type AckError = SendError;
//     type Context = Ctx;
//     async fn ack(
//         &mut self,
//         ctx: &Self::Context,
//         result: &Response<Res, Ctx>,
//     ) -> Result<(), Self::AckError> {
//         let ctx = ctx.clone();
//         // let res = result.map(|res| Cdc::encode(res).unwrap());
//         // self.send((ctx, res)).await.unwrap();
//         Ok(())
//     }
// }

// /// A layer that acknowledges a job completed successfully
// #[derive(Debug)]
// pub struct AckLayer<A, Req, Ctx, Cdc> {
//     ack: A,
//     job_type: PhantomData<Request<Req, Ctx>>,
//     codec: PhantomData<Cdc>,
// }

// impl<A, Req, Ctx, Cdc> AckLayer<A, Req, Ctx, Cdc> {
//     /// Build a new [AckLayer] for a job
//     pub fn new(ack: A) -> Self {
//         Self {
//             ack,
//             job_type: PhantomData,
//             codec: PhantomData,
//         }
//     }
// }

// impl<A, Req, Ctx, S, Cdc> Layer<S> for AckLayer<A, Req, Ctx, Cdc>
// where
//     S: Service<Request<Req, Ctx>> + Send + 'static,
//     S::Error: std::error::Error + Send + Sync + 'static,
//     S::Future: Send + 'static,
//     A: Ack<Req, S::Response, Cdc> + Clone + Send + Sync + 'static,
// {
//     type Service = AckService<S, A, Req, Ctx, Cdc>;

//     fn layer(&self, service: S) -> Self::Service {
//         AckService {
//             service,
//             ack: self.ack.clone(),
//             job_type: PhantomData,
//             codec: PhantomData,
//         }
//     }
// }

// /// The underlying service for an [AckLayer]
// #[derive(Debug)]
// pub struct AckService<SV, A, Req, Ctx, Cdc> {
//     service: SV,
//     ack: A,
//     job_type: PhantomData<Request<Req, Ctx>>,
//     codec: PhantomData<Cdc>,
// }

// impl<Sv: Clone, A: Clone, Req, Ctx, Cdc> Clone for AckService<Sv, A, Req, Ctx, Cdc> {
//     fn clone(&self) -> Self {
//         Self {
//             ack: self.ack.clone(),
//             job_type: PhantomData,
//             service: self.service.clone(),
//             codec: PhantomData,
//         }
//     }
// }

// impl<SV, A, Req, Ctx, Cdc> Service<Request<Req, Ctx>> for AckService<SV, A, Req, Ctx, Cdc>
// where
//     SV: Service<Request<Req, Ctx>> + Send + 'static,
//     SV::Error: Into<BoxDynError> + Send + 'static,
//     SV::Future: Send + 'static,
//     A: Ack<Req, SV::Response, Cdc, Context = Ctx> + Send + 'static + Clone,
//     Req: 'static + Send,
//     SV::Response: std::marker::Send + Serialize,
//     <A as Ack<Req, SV::Response, Cdc>>::Context: Send + Clone,
//     <A as Ack<Req, SV::Response, Cdc>>::Context: 'static,
//     Ctx: Clone,
// {
//     type Response = SV::Response;
//     type Error = Error;
//     type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

//     fn poll_ready(
//         &mut self,
//         cx: &mut std::task::Context<'_>,
//     ) -> std::task::Poll<Result<(), Self::Error>> {
//         self.service
//             .poll_ready(cx)
//             .map_err(|e| Error::Failed(Arc::new(e.into())))
//     }

//     fn call(&mut self, request: Request<Req, Ctx>) -> Self::Future {
//         let mut ack = self.ack.clone();
//         let ctx = request.parts.context.clone();
//         let attempt = request.parts.attempt.clone();
//         let task_id = request.parts.task_id.clone();
//         let fut = self.service.call(request);
//         let fut_with_ack = async move {
//             let res = fut.await.map_err(|err| {
//                 let e: BoxDynError = err.into();
//                 // Try to downcast the error to see if it is already of type `Error`
//                 if let Some(custom_error) = e.downcast_ref::<Error>() {
//                     return custom_error.clone();
//                 }
//                 Error::Failed(Arc::new(e))
//             });
//             // let response = Response {
//             //     attempt,
//             //     inner: res,
//             //     task_id,
//             //     _priv: (),
//             // };
//             // if let Err(_e) = ack.ack(&ctx, &response).await {
//             //     // TODO: Implement tracing in apalis core
//             //     // tracing::error!("Acknowledgement Failed: {}", e);
//             // }
//             // response.inner
//             todo!()
//         };
//         fut_with_ack.boxed()
//     }
// }

use tower::{layer::util::Stack, Layer};

use crate::{
    worker::builder::WorkerBuilder,
    error::BoxDynError,
    request::{Parts, Request},
};

use super::long_running::LongRunningLayer;

pub trait AcknowledgementExt<Args, Ctx, Source, Middleware, Ack>: Sized {
    fn ack_with(self, ack: Ack) -> WorkerBuilder<Args, Ctx, Source, Stack<LongRunningLayer, Middleware>>;
}

trait AcknowledgeTask<Res, Ctx> {
    type Error;
    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        parts: &Parts<Ctx>,
    ) -> Result<(), Self::Error>;
}

impl<Args, P, M, Ctx, Ack> AcknowledgementExt<Args, Ctx, P, M, Ack>
    for WorkerBuilder<Args, Ctx, P, M>
where
    M: Layer<LongRunningLayer>,
{
    fn ack_with(
        self,
        ack: Ack,
    ) -> WorkerBuilder<Args, Ctx, P, Stack<LongRunningLayer, M>> {
        let this = self.layer(LongRunningLayer);
        WorkerBuilder {
            name: this.name,
            request: this.request,
            layer: this.layer,
            source: this.source,
            shutdown: this.shutdown,
            event_handler: this.event_handler,
        }
    }
}
