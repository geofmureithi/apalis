use actix_web::{web, App, Error, HttpResponse, HttpServer, ResponseError};
use apalis::{
    redis::RedisStorage, Job, JobContext, JobError, JobRequest, JobResult, Monitor, Storage,
    WorkerBuilder,
};
use futures::future;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct Email {
    to: String,
    subject: String,
    text: String,
}

impl Job for Email {
    const NAME: &'static str = "redis::Email";
}

async fn email_service(job: Email, ctx: JobContext) -> Result<JobResult, JobError> {
    // Do something awesome
    println!("Attempting to send email to {}", job.to);
    Ok(JobResult::Success)
}

async fn push_email(
    email: web::Json<Email>,
    storage: web::Data<RedisStorage<Email>>,
) -> HttpResponse {
    let storage = &*storage.into_inner();
    let mut storage = storage.clone();
    let res = storage.push(email.into_inner()).await;
    match res {
        Ok(()) => HttpResponse::Ok().body(format!("Email added to queue")),
        Err(e) => HttpResponse::InternalServerError().body(format!("{}", e)),
    }
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    let storage = RedisStorage::connect("redis://127.0.0.1/").await.unwrap();
    let data = web::Data::new(storage.clone());
    let http = HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .service(web::scope("/emails").route("/push", web::post().to(push_email)))
    })
    .bind("127.0.0.1:8000")?
    .run();

    let worker = Monitor::new()
        .register_with_count(2, move |_| {
            WorkerBuilder::new(storage.clone())
                .build_fn(email_service)
                .start()
        })
        .run();
    future::try_join(http, worker).await?;

    Ok(())
}
