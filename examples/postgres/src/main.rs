use apalis::{
    layers::TraceLayer, postgres::PostgresStorage, Monitor, Storage, WorkerBuilder, WorkerEvent,
    WorkerFactoryFn, WorkerListener,
};

use email_service::{send_email, Email};

async fn produce_jobs(storage: &PostgresStorage<Email>) {
    let mut storage = storage.clone();
    for i in 0..10 {
        storage
            .push(Email {
                to: format!("test{}@example.com", i),
                text: "Test backround job from Apalis".to_string(),
                subject: "Background email job".to_string(),
            })
            .await
            .unwrap();
    }
}

struct TracingListener;
impl WorkerListener for TracingListener {
    fn on_event(&self, worker_id: &String, event: &WorkerEvent) {
        tracing::info!(worker_id = ?worker_id, event = ?event, "Received message from worker")
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "debug,sqlx::query=error");
    tracing_subscriber::fmt::init();
    let database_url = std::env::var("DATABASE_URL").expect("Must specify path to db");

    let pg: PostgresStorage<Email> = PostgresStorage::connect(database_url).await.unwrap();
    pg.setup()
        .await
        .expect("unable to run migrations for postgres");

    produce_jobs(&pg).await;

    Monitor::new()
        .register_with_count(4, move |_| {
            WorkerBuilder::new(pg.clone())
                .layer(TraceLayer::new())
                .build_fn(send_email)
        })
        .event_handler(TracingListener)
        .run()
        .await
}
