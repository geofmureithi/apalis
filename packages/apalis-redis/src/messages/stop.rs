use apalis::{Job, JobHandler};
use crate::consumer::RedisConsumer;
use actix::prelude::*;

#[derive(Message)]
#[rtype(result = "()")]
pub struct Stop;

impl<J: 'static + Unpin + JobHandler<Self>> Handler<Stop> for RedisConsumer<J>
where
    J: Job,
{
    type Result = ();

    fn handle(&mut self, _: Stop, ctx: &mut Self::Context) -> Self::Result {
        ctx.stop();
    }
}
