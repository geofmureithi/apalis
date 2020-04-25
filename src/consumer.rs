use crate::actor::{EnqueueJobs, FetchJobs, QueueActor, RegisterConsumer};
use crate::message::{MessageDecodable, MessageEncodable, MessageGuard};
use actix::clock::{interval_at, Duration, Instant};
use actix::prelude::*;
use futures::stream::StreamExt;
use log::{debug, info};
use redis::{from_redis_value, Value};

#[derive(Message)]
#[rtype(result = "()")]
struct HeartBeat;

#[derive(Message)]
#[rtype(result = "()")]
struct Stop;

#[derive(Message)]
#[rtype(result = "()")]
struct Schedule;

#[derive(Message)]
#[rtype(result = "()")]
struct Fetch;

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct Jobs<T>(pub Vec<T>);

pub struct Consumer<T>
where
    T: MessageDecodable,
{
    addr: Addr<QueueActor>,
    processor: Recipient<Jobs<T>>,
    id: String,
}

impl<T: MessageDecodable + 'static> Consumer<T> {
    pub fn new(addr: Addr<QueueActor>, processor: Recipient<Jobs<T>>) -> Self {
        Consumer {
            addr,
            processor,
            id: String::from("consumer_1"),
        }
    }
}

impl<T: MessageDecodable + 'static> StreamHandler<HeartBeat> for Consumer<T> {
    fn handle(&mut self, _: HeartBeat, ctx: &mut Context<Consumer<T>>) {
        info!("Received heartbeat for consumer: {:?}", self.id);
    }

    fn finished(&mut self, ctx: &mut Self::Context) {
        debug!("finished");
    }
}

impl<T: MessageDecodable + 'static> StreamHandler<Schedule> for Consumer<T> {
    fn handle(&mut self, _: Schedule, ctx: &mut Context<Consumer<T>>) {
        let queue = self.addr.clone();
        actix::spawn(async move {
            let res = queue.send(EnqueueJobs(10)).await;
            match res {
                Ok(Ok(count)) => {
                    info!("Jobs queued: {:?}", count);
                }
                Ok(Err(e)) => {
                    debug!("Redis Enque job failed: {:?}", e);
                }
                Err(e) => {
                    debug!("Unable to Enqueue jobs, Error: {:?}", e);
                }
            }
        });
    }

    fn finished(&mut self, ctx: &mut Self::Context) {
        info!("finished");
    }
}

impl<T: MessageDecodable + 'static> StreamHandler<Fetch> for Consumer<T> {
    fn handle(&mut self, _: Fetch, ctx: &mut Context<Consumer<T>>) {
        let queue = self.addr.clone();
        let id = self.id.clone();
        let processor = self.processor.clone();
        actix::spawn(async move {
            let res = queue
                .send(FetchJobs {
                    count: 10,
                    consumer_id: id,
                })
                .await;
            match res {
                Ok(Ok(jobs)) => {
                    debug!("Fetched jobs: {:?}", jobs);
                    let tasks: Vec<Option<Result<_, &str>>> = jobs
                        .into_iter()
                        .map(|j| {
                            let j = match j {
                                j @ Value::Data(_) => j,
                                _ => {
                                    return Some(Err("unknown result type for next message"));
                                }
                            };
                            match T::decode_message(&j) {
                                Err(e) => {
                                    println!("{:?}", e);
                                    Some(Err(e))
                                }
                                Ok(message) => {
                                    // let message = MessageGuard::new(
                                    //     message,
                                    //     from_redis_value(&j).unwrap(),
                                    // );
                                    Some(Ok(message))
                                }
                            }
                        })
                        .collect();
                    let tasks: Vec<T> = tasks
                        .into_iter()
                        .map(|t| {
                            let msg = t.unwrap();
                            let msg = msg.unwrap();
                            msg
                        })
                        .collect();
                    if tasks.len() > 0 {
                        processor.send(Jobs(tasks)).await.unwrap();
                    }
                }
                Ok(Err(e)) => {
                    debug!("Redis Fetch jobs failed: {:?}", e);
                }
                Err(e) => {
                    debug!("Unable to Fetch jobs, Error: {:?}", e);
                }
            }
        });
    }

    fn finished(&mut self, ctx: &mut Self::Context) {
        info!("finished");
    }
}

impl<T: MessageDecodable + 'static> Handler<Stop> for Consumer<T> {
    type Result = ();

    fn handle(&mut self, _: Stop, ctx: &mut Self::Context) -> Self::Result {
        ctx.stop();
    }
}

impl<T: MessageDecodable + 'static> Actor for Consumer<T> {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        let queue_actor = self.addr.clone();
        let id = self.id.clone();
        let this = ctx.address().clone();
        actix::spawn(async move {
            let reg = queue_actor.send(RegisterConsumer(id)).await;
            match reg {
                Ok(Ok(Some(true))) => {
                    info!("Consumer successfully registered");
                }
                _ => {
                    this.send(Stop).await.unwrap();
                }
            };
        });
        // add stream
        let start = Instant::now() + Duration::from_millis(50);
        let heart_beat = interval_at(start, Duration::from_secs(30)).map(|_| HeartBeat);
        Self::add_stream(heart_beat, ctx);
        info!("Added consumer heartbeat");
        let schedule = interval_at(start, Duration::from_secs(10)).map(|_| Schedule);
        Self::add_stream(schedule, ctx);
        info!("Added consumer scheduler");
        let fetch = interval_at(start, Duration::from_secs(10)).map(|_| Fetch);
        Self::add_stream(fetch, ctx);
        info!("Added consumer fetcher");
    }
}

// To use actor with supervisor actor has to implement `Supervised` trait
impl<T: MessageDecodable + 'static> actix::Supervised for Consumer<T> {
    fn restarting(&mut self, ctx: &mut Context<Consumer<T>>) {
        debug!("Restarting Consumer");
    }
}
