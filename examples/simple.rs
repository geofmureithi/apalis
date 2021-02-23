use actix::prelude::*;
use log::info;

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
extern crate actix_redis_jobs;
use std::collections::HashMap;

use actix_redis_jobs::{
    Consumer, JobContext, JobHandler, JobResult, Producer, RedisStorage, ScheduleJob, Worker,
};

struct EmailActor;

impl Actor for EmailActor {
    type Context = Context<Self>;
}

impl JobHandler for Email {
    fn handle(&self, ctx: &JobContext) -> BoxFuture<JobResult> {
        let addr = ctx.data_opt::<Addr<EmailActor>>().unwrap();
        let addr = addr.clone();
        let fut = async move {
            match self {
                Email::Mailchimp(_m) => JobResult::Retry(actix_redis_jobs::TaskError::External(
                    "No Mailchimp handler".to_string(),
                )),
                Email::Sendgrid(sendgrid) => {
                    addr.send(sendgrid.clone()).await.unwrap();
                    JobResult::Result(Ok(()))
                }
            }
        };
        Box::pin(fut)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Message)]
#[rtype(result = "()")]
struct Car;

impl JobHandler for Car {
    fn handle(&self, _ctx: &JobContext) -> BoxFuture<JobResult> {
        let fut = async move {
            info!("Got new car job");
            JobResult::Result(Ok(()))
        };
        Box::pin(fut)
    }
}

impl Handler<Sendgrid> for EmailActor {
    type Result = ();

    fn handle(&mut self, job: Sendgrid, ctx: &mut Self::Context) -> Self::Result {
        let mut cool_header = HashMap::with_capacity(2);
        use sendgrid::v3::*;
        cool_header.insert(String::from("x-cool"), String::from("indeed"));
        cool_header.insert(String::from("x-cooler"), String::from("cold"));

        let p = Personalization::new(Email::new("test@fuse.co.ke")).add_headers(cool_header);

        let m = Message::new(Email::new("mureithinjuguna@gmail.com"))
            .set_subject("Subject")
            .add_content(
                Content::new()
                    .set_content_type("text/html")
                    .set_value("Test"),
            )
            .add_personalization(p);

        let api_key = job.api_key;
        let sender = Sender::new(api_key);
        let fut = async move {
            let resp = sender.send(&m).await.unwrap();
            println!("status: {}", resp.status());
        }
        .into_actor(self);
        ctx.spawn(fut);
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Message)]
#[rtype(result = "()")]
struct Sendgrid {
    api_key: String,
    subject: String,
    to: String,
    message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Message)]
#[rtype(result = "()")]
struct Mailchimp {
    x_api_key: String,
    subject: String,
    to: String,
    message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Message)]
#[rtype(result = "()")]
enum Email {
    Sendgrid(Sendgrid),
    Mailchimp(Mailchimp),
}

#[actix_rt::main]
async fn main() {
    // std::env::set_var("RUST_LOG", "info");
    // env_logger::init();
    // let storage = RedisStorage::new("redis://127.0.0.1/").await.unwrap();
    // let producer = Producer::start(&storage, "emails");

    // let task1 = Email::Sendgrid(Sendgrid {
    //     api_key: "uu"
    //         .to_string(),
    //     subject: String::new(),
    //     to: String::new(),
    //     message: String::new(),
    // });

    // // let task2 = Email::Sendgrid(Sendgrid {
    // //     api_key: "scheduled".to_string(),
    // //     subject: String::new(),
    // //     to: String::new(),
    // //     message: String::new(),
    // // });
    // //let scheduled = ScheduleJob::new(task1).in_minutes(1);
    // producer.send(task1).await.unwrap();
    // //producer.send(task2).await.unwrap();

    // let emails: Consumer<Email> = Consumer::new(&storage, "emails").data(EmailActor.start());
    // let cars: Consumer<Car> = Consumer::new(&storage, "cars");
    Worker::start(|srv| srv.workers(4))
        // .add_consumer(emails)
        // .add_consumer(cars)
        ;
}
