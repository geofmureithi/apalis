use actix::{Actor, Addr, Handler, Message, StreamHandler};
use actix_web::{web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;
use apalis::{Job, JobHandler};
use apalis_redis::RedisProducer;
use apalis_redis::{RedisConsumer, RedisJobContext};
use serde::{Deserialize, Serialize};
use std::sync::Mutex;


//Define our job
#[derive(Serialize, Deserialize, Debug, Message, Clone)]
#[rtype(result = "()")]
enum WSJob {
    Notify(String),
}

impl Job for WSJob {
    type Result = ();
}

impl JobHandler<RedisConsumer<WSJob>> for WSJob {
    type Result = ();
    fn handle(self, ctx: &mut RedisJobContext<Self>) {
        let addrs = ctx.data_opt::<web::Data<Mutex<Vec<Addr<MyWs>>>>>().unwrap();
        let addrs = addrs.lock().unwrap();
        let addrs = addrs.clone();
        for addr in addrs {
            addr.do_send(self.clone()); //Send result to all users
        }
    }
}

/// Define HTTP actor
struct MyWs {
    producer: Addr<RedisProducer<WSJob>>, //Allow an actor to send new jobs
}

impl Actor for MyWs {
    type Context = ws::WebsocketContext<Self>;
}

impl Handler<WSJob> for MyWs {
    type Result = ();
    fn handle(&mut self, job: WSJob, ctx: &mut Self::Context) {
        let text = match job {
            WSJob::Notify(text) => text,
        };
        ctx.text(text);
    }
}

/// Handler for ws::Message message
impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for MyWs {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Text(text)) => {
                let producer = self.producer.clone();
                producer.do_send(WSJob::Notify(text).into()); //Send to redis
            }
            Ok(ws::Message::Binary(bin)) => ctx.binary(bin),
            _ => (),
        }
    }
}

//Define a websocket 

async fn index(
    req: HttpRequest,
    producer: web::Data<Addr<RedisProducer<WSJob>>>,
    addrs: web::Data<Mutex<Vec<Addr<MyWs>>>>,
    stream: web::Payload,
) -> Result<HttpResponse, Error> {
    let (addr, resp) = ws::start_with_addr(
        MyWs {
            producer: producer.as_ref().clone(),
        },
        &req,
        stream,
    )?;
    let mut addrs = addrs.lock().unwrap();
    addrs.push(addr);
    Ok(resp)
}



#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();

    let conn = "redis://127.0.0.1/";

    let addrs: web::Data<Mutex<Vec<Addr<MyWs>>>> = web::Data::new(Mutex::new(vec![]));
    let consumer = RedisConsumer::<WSJob>::create(conn)
        .expect("Couldnt start consumer")
        .data(addrs.clone());
    let producer = web::Data::new(consumer.build_producer());
    Actor::create(|_ctx| consumer); //Start a standalone consumer
    
    HttpServer::new(move || {
        App::new()
            .app_data(producer.clone())
            .app_data(addrs.clone())
            .route("/", web::get().to(index))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
