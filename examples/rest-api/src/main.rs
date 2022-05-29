use std::{collections::HashSet, time::Duration};

use actix_cors::Cors;
use actix_web::{
    dev::HttpServiceFactory, web, App, Error, HttpRequest, HttpResponse, HttpServer, Responder,
    ResponseError, Scope,
};
use apalis::{
    redis::RedisStorage, sqlite::SqliteStorage, Job, JobContext, JobError, JobRequest, JobResult,
    JobState, Monitor, Storage, StorageJobExt, WorkerBuilder,
};
use futures::future;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use email_service::{send_email, Email};

#[derive(Debug, Deserialize, Serialize)]
struct Notification {
    text: String,
}

impl Job for Notification {
    const NAME: &'static str = "sqlite::Notification";
}

async fn notification_service(notif: Notification, ctx: JobContext) -> Result<JobResult, JobError> {
    println!("Attempting to send notification {}", notif.text);
    actix_rt::time::sleep(Duration::from_secs(5)).await;
    Ok(JobResult::Success)
}

async fn push_job<J>(email: web::Json<J>, storage: web::Data<RedisStorage<J>>) -> HttpResponse
where
    J: Job + Serialize + DeserializeOwned + 'static,
{
    let storage = &*storage.into_inner();
    let mut storage = storage.clone();
    let res = storage.push(email.into_inner()).await;
    match res {
        Ok(()) => HttpResponse::Ok().body(format!("Email added to queue")),
        Err(e) => HttpResponse::InternalServerError().body(format!("{}", e)),
    }
}

#[derive(Deserialize)]
struct JobId {
    job_id: String,
}

async fn get_job<J>(job: web::Path<JobId>, storage: web::Data<RedisStorage<J>>) -> HttpResponse
where
    J: Job + Serialize + DeserializeOwned + 'static,
{
    let storage = &*storage.into_inner();
    let storage = storage.clone();
    let res = storage.fetch_by_id(job.job_id.to_string()).await;
    match res {
        Ok(Some(job)) => HttpResponse::Ok().json(job),
        Ok(None) => HttpResponse::NotFound().finish(),
        Err(e) => HttpResponse::InternalServerError().body(format!("{}", e)),
    }
}

trait StorageRest<J>: Storage<Output = J> {
    fn name(&self) -> String;
    fn scope(&self) -> Scope;
}

impl<J, T> StorageRest<J> for T
where
    T: Storage<Output = J> + StorageJobExt<J>,
    J: Job + Serialize + DeserializeOwned + 'static,
{
    fn name(&self) -> String {
        J::NAME.to_string()
    }

    fn scope(&self) -> Scope {
        let name = self.name().to_string();
        let slug = slug::slugify(&name);
        let slug = format!("/{}", slug);
        Scope::new(&slug).route("/job/{job_id}", web::get().to(get_job::<J>))
    }
}

#[derive(Debug, Deserialize, Serialize, Default)]
struct Counts {
    latest: i32,
    active: i32,
    pending: i32,
    completed: i32,
    failed: i32,
    scheduled: i32,
    killed: i32,
}
#[derive(Debug, Deserialize, Serialize, Default)]
struct Pagination {
    pageCount: i32,
    range: Range,
}

#[derive(Debug, Deserialize, Serialize, Default)]
struct Range {
    start: i32,
    end: i32,
}

#[derive(Debug, Deserialize, Serialize)]
struct Queue {
    name: String,
    url: String,
    jobs: serde_json::Value,
    counts: Counts,
    //pagination: Pagination,
}

#[derive(Debug, Deserialize, Serialize)]
struct QueueList {
    set: HashSet<String>,
}

struct StorageApiBuilder {
    scope: Scope,
    list: QueueList,
}

impl StorageApiBuilder {
    fn add_storage<J, S>(mut self, storage: S) -> Self
    where
        J: Job + Serialize + DeserializeOwned + 'static,
        S: StorageRest<J> + StorageJobExt<J>,
        S: Storage<Output = J>,
        S: 'static,
    {
        let name = J::NAME.to_string();
        self.list.set.insert(name);
        Self {
            scope: self
                .scope
                .app_data(storage.clone())
                .service(storage.scope()),
            list: self.list,
        }
    }

    fn to_scope(self) -> Scope {
        #[derive(Deserialize)]
        struct Filter {
            #[serde(default)]
            status: JobState,
            page: i32,
            active_queue: Option<String>,
        }

        fn get_storage<S, T>(req: &HttpRequest) -> S
        where
            S: 'static + StorageJobExt<T>,
        {
            let storage: &S = req.app_data().unwrap();
            storage.clone()
        }
        async fn get_jobs_from_queue(
            slug: String,
            req: &HttpRequest,
            filter: &Filter,
        ) -> serde_json::Value {
            match slug.as_str() {
                "redis-email" => {
                    let mut storage: RedisStorage<Email> = get_storage(&req);
                    let jobs = storage
                        .list_jobs(&filter.status, filter.page)
                        .await
                        .unwrap();
                    serde_json::to_value(jobs).unwrap()
                }
                "sqlite-notification" => {
                    let mut storage: SqliteStorage<Notification> = get_storage(&req);
                    let jobs = storage
                        .list_jobs(&filter.status, filter.page)
                        .await
                        .unwrap();
                    serde_json::to_value(jobs).unwrap()
                }
                _ => unimplemented!(),
            }
        }
        async fn fetch_queues(
            queues: web::Data<QueueList>,
            req: HttpRequest,
            filter: web::Query<Filter>,
        ) -> HttpResponse {
            let mut queue_result = Vec::new();
            for queue in &queues.set {
                let queue_slug = slug::slugify(queue);
                let jobs = get_jobs_from_queue(queue_slug.clone(), &req, &filter).await;

                queue_result.push(Queue {
                    url: queue_slug,
                    name: queue.clone(),
                    jobs,
                    counts: Counts {
                        latest: 10,
                        ..Default::default()
                    },
                    //pagination: Pagination::default(),
                })
            }
            #[derive(Serialize)]
            struct Res {
                queues: Vec<Queue>,
            }

            HttpResponse::Ok().json(Res {
                queues: queue_result,
            })
        }

        self.scope
            .app_data(web::Data::new(self.list))
            .route("", web::get().to(fetch_queues))
    }

    fn new() -> Self {
        Self {
            scope: Scope::new("queues"),
            list: QueueList {
                set: HashSet::new(),
            },
        }
    }
}

async fn produce_redis_jobs(mut storage: RedisStorage<Email>) {
    for i in 0..10 {
        storage
            .schedule(
                Email {
                    to: "test@example.com".to_string(),
                    text: "Test backround job from Apalis".to_string(),
                    subject: "Background email job".to_string(),
                },
                chrono::Utc::now(),
            )
            .await
            .unwrap();
    }
}
async fn produce_sqlite_jobs(mut storage: SqliteStorage<Notification>) {
    for i in 0..10 {
        storage
            .push(Notification {
                text: format!("Notiification: {}", i),
            })
            .await
            .unwrap();
    }
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "warn");
    env_logger::init();

    let storage = RedisStorage::connect("redis://127.0.0.1/").await.unwrap();
    let sqlite = SqliteStorage::connect("sqlite::memory:").await.unwrap();
    sqlite.setup().await;
    let worker_storage = storage.clone();
    let sqlite_storage = sqlite.clone();
    // produce_redis_jobs(storage.clone()).await;
    produce_sqlite_jobs(sqlite.clone()).await;
    let http = HttpServer::new(move || {
        App::new().wrap(Cors::permissive()).service(
            web::scope("/api").service(
                StorageApiBuilder::new()
                    .add_storage(storage.clone())
                    .add_storage(sqlite.clone())
                    .to_scope(),
            ),
        )
    })
    .bind("127.0.0.1:8000")?
    .run();

    let worker = Monitor::new()
        .register_with_count(2, move |_| {
            WorkerBuilder::new(worker_storage.clone()).build_fn(send_email)
        })
        .register_with_count(1, move |_| {
            WorkerBuilder::new(sqlite_storage.clone()).build_fn(notification_service)
        })
        .run();
    future::try_join(http, worker).await?;

    Ok(())
}
