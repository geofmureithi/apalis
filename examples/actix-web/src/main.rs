use actix_web::rt::signal;
use actix_web::{web, App, HttpResponse, HttpServer};
use anyhow::Result;
use apalis::prelude::*;
use apalis::utils::TokioExecutor;
use apalis::{layers::tracing::TraceLayer, redis::RedisStorage};
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
        Ok(jid) => HttpResponse::Ok().body(format!("Email with job_id [{jid}] added to queue")),
        Err(e) => HttpResponse::InternalServerError().body(format!("{e}")),
    }
}

#[actix_web::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    let conn = apalis::redis::connect("redis://127.0.0.1/").await?;
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
    let worker = Monitor::<TokioExecutor>::new()
        .register_with_count(2, {
            WorkerBuilder::new("tasty-avocado")
                .layer(TraceLayer::new())
                .with_storage(storage)
                .build_fn(send_email)
        })
        .run_with_signal(signal::ctrl_c());

    future::try_join(http, worker).await?;
    Ok(())
}
