use actix_web::rt::signal;
use actix_web::{web, App, HttpResponse, HttpServer};
use anyhow::Result;
use apalis::prelude::*;

use apalis_redis::RedisStorage;
use futures::future;

use email_service::{send_email, Email};

async fn push_email(
    email: web::Json<Email>,
    storage: web::Data<RedisStorage<Email>>,
) -> HttpResponse {
    let storage = &*storage.into_inner();
    let mut storage = storage.clone();
    let res = storage.push(email.into_inner()).await;
    match res {
        Ok(ctx) => HttpResponse::Ok().json(ctx),
        Err(e) => HttpResponse::InternalServerError().body(format!("{e}")),
    }
}

#[actix_web::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    let redis_url = std::env::var("REDIS_URL").expect("Missing env variable REDIS_URL");
    let conn = apalis_redis::connect(redis_url)
        .await
        .expect("Could not connect");
    let storage = RedisStorage::new(conn);
    let data = web::Data::new(storage.clone());
    let http = async {
        HttpServer::new(move || {
            App::new()
                .app_data(data.clone())
                .service(web::scope("/emails").route("/push", web::post().to(push_email)))
        })
        .bind("127.0.0.1:8000")?
        .run()
        .await?;
        Ok(())
    };
    let worker = Monitor::new()
        .register({
            WorkerBuilder::new("tasty-avocado")
                .enable_tracing()
                // .concurrency(2)
                .backend(storage)
                .build_fn(send_email)
        })
        .run_with_signal(signal::ctrl_c());

    future::try_join(http, worker).await?;
    Ok(())
}
