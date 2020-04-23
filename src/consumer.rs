use crate::actor::{EnqueueJobs, FetchJobs, QueueActor, RegisterConsumer};
use crate::message::{MessageDecodable, MessageEncodable, MessageGuard};
use actix::clock::{interval_at, Duration, Instant};
use actix::prelude::*;
use futures::stream::StreamExt;
use log::{debug, info};
use redis::{from_redis_value, Value};

pub type ConsumerItem = MessageGuard<dyn MessageEncodable>;

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

#[derive(Message)]
#[rtype(result = "()")]
pub struct Jobs(pub Vec<ConsumerItem>);

pub struct Consumer {
    addr: Addr<QueueActor>,
    processor: Recipient<Jobs>,
    id: String,
}

impl Consumer {
    pub fn new(addr: Addr<QueueActor>, processor: Recipient<Jobs>) -> Self {
        Consumer {
            addr,
            processor,
            id: String::from("consumer_1"),
        }
    }
}

impl StreamHandler<HeartBeat> for Consumer {
    fn handle(&mut self, _: HeartBeat, ctx: &mut Context<Consumer>) {
        info!("Received heartbeat for consumer: {:?}", self.id);
    }

    fn finished(&mut self, ctx: &mut Self::Context) {
        debug!("finished");
    }
}

impl StreamHandler<Schedule> for Consumer {
    fn handle(&mut self, _: Schedule, ctx: &mut Context<Consumer>) {
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

impl StreamHandler<Fetch> for Consumer {
    fn handle(&mut self, _: Fetch, ctx: &mut Context<Consumer>) {
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
                    println!("Fetched jobs: {:?}", jobs);
                    let tasks: Vec<Option<Result<ConsumerItem, &str>>> = jobs
                        .into_iter()
                        .map(|j| {
                            let j = match j {
                                j @ Value::Data(_) => j,
                                _ => {
                                    return Some(Err("unknown result type for next message"));
                                }
                            };
                            match MessageDecodable::decode_message(&j) {
                                Err(e) => {
                                    println!("{:?}", e);
                                    Some(Err(e))
                                }
                                Ok(message) => Some(Ok(MessageGuard::new(
                                    message,
                                    from_redis_value(&j).unwrap(),
                                ))),
                            }
                        })
                        .collect();
                    let tasks: Vec<ConsumerItem> = tasks
                        .into_iter()
                        .map(|t| {
                            let msg = t.unwrap();
                            let msg = msg.unwrap();
                            msg
                        })
                        .collect();

                    processor.send(Jobs(tasks)).await.unwrap();
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

impl Handler<Stop> for Consumer {
    type Result = ();

    fn handle(&mut self, _: Stop, ctx: &mut Self::Context) -> Self::Result {
        ctx.stop();
    }
}

impl Actor for Consumer {
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

        let schedule = interval_at(start, Duration::from_secs(10)).map(|_| Schedule);
        Self::add_stream(schedule, ctx);

        let fetch = interval_at(start, Duration::from_secs(10)).map(|_| Fetch);
        Self::add_stream(fetch, ctx);
    }
}

// To use actor with supervisor actor has to implement `Supervised` trait
impl actix::Supervised for Consumer {
    fn restarting(&mut self, ctx: &mut Context<Consumer>) {
        debug!("Restarting Consumer");
    }
}
