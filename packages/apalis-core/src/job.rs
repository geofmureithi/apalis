use futures::future::BoxFuture;

use crate::{context::JobContext, response::JobResponse};

pub type JobFuture<I> = BoxFuture<'static, I>;

pub trait Job: Sized {
    type Result: JobResponse + 'static;
    const NAME: &'static str; // = std::any::type_name::<Self>()
    fn handle(&self, ctx: &JobContext) -> Self::Result
    where
        Self: Sized;
}
