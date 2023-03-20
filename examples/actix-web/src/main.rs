use actix_web::{web, App, HttpResponse, HttpServer};
use anyhow::Result;
use apalis::prelude::*;
use apalis::{layers::TraceLayer, redis::RedisStorage};
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
        Ok(()) => HttpResponse::Ok().body("Email added to queue".to_string()),
        Err(e) => HttpResponse::InternalServerError().body(format!("{e}")),
    }
}

#[actix_web::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    let storage = RedisStorage::connect("redis://127.0.0.1/").await?;
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
        .register_with_count(2, move |c| {
            WorkerBuilder::new(format!("tasty-avocado-{c}"))
                .layer(TraceLayer::new())
                .with_storage(storage.clone())
                .build_fn(send_email)
        })
        .run();

    future::try_join(http, worker).await?;
    Ok(())
}
