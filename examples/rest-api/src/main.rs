use actix_web::rt::signal;
use actix_web::{web, App, HttpResponse, HttpServer};
use anyhow::Result;
use apalis::prelude::*;

use apalis_redis::RedisStorage;
use futures::future;

use email_service::{send_email, Email};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug)]
struct Filter {
    #[serde(default)]
    pub status: State,
    #[serde(default = "default_page")]
    pub page: i32,
}

fn default_page() -> i32 {
    1
}

#[derive(Debug, Serialize, Deserialize)]
struct GetJobsResult<T> {
    pub stats: Stat,
    pub jobs: Vec<T>,
}

async fn push_job(job: web::Json<Email>, storage: web::Data<RedisStorage<Email>>) -> HttpResponse {
    let mut storage = (**storage).clone();
    let res = storage.push(job.into_inner()).await;
    match res {
        Ok(parts) => {
            HttpResponse::Ok().body(format!("Job with ID [{}] added to queue", parts.task_id))
        }
        Err(e) => HttpResponse::InternalServerError().json(e.to_string()),
    }
}

async fn get_jobs(
    storage: web::Data<RedisStorage<Email>>,
    filter: web::Query<Filter>,
) -> HttpResponse {
    let stats = storage.stats().await.unwrap_or_default();
    let res = storage.list_jobs(&filter.status, filter.page).await;
    match res {
        Ok(jobs) => HttpResponse::Ok().json(GetJobsResult { stats, jobs }),
        Err(e) => HttpResponse::InternalServerError().json(e.to_string()),
    }
}

async fn get_workers(storage: web::Data<RedisStorage<Email>>) -> HttpResponse {
    let workers = storage.list_workers().await;
    match workers {
        Ok(workers) => HttpResponse::Ok().json(workers),
        Err(e) => HttpResponse::InternalServerError().json(e.to_string()),
    }
}

async fn get_job(
    job_id: web::Path<TaskId>,
    storage: web::Data<RedisStorage<Email>>,
) -> HttpResponse {
    let mut storage = (**storage).clone();

    let res = storage.fetch_by_id(&job_id).await;
    match res {
        Ok(Some(job)) => HttpResponse::Ok().json(job),
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(e) => HttpResponse::InternalServerError().json(e.to_string()),
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
                .route("/", web::get().to(get_jobs)) // Fetch jobs in queue
                .route("/workers", web::get().to(get_workers)) // Fetch workers
                .route("/job", web::put().to(push_job)) // Allow add jobs via api
                .route("/job/{job_id}", web::get().to(get_job)) // Allow fetch specific job
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
                .backend(storage)
                .build_fn(send_email)
        })
        .run_with_signal(signal::ctrl_c());

    future::try_join(http, worker).await?;
    Ok(())
}
