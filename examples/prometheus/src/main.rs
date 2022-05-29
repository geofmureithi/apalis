//! Run with
//!
//! ```not_rust
//! cd examples && cargo run -p prometheus-example
//! ```

use apalis::{
    layers::PrometheusLayer, redis::RedisStorage, Job, JobContext, JobError, JobResult, Monitor,
    Storage, WorkerBuilder,
};
use axum::{
    extract::Form,
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::get,
    Extension, Router,
};
use futures::future::ready;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{fmt::Debug, io::Error, net::SocketAddr};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use email_service::{send_email, Email, FORM_HTML};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();
    let storage: RedisStorage<Email> = RedisStorage::connect("redis://127.0.0.1/").await.unwrap();
    // build our application with some routes
    let recorder_handle = setup_metrics_recorder();
    let app = Router::new()
        .route("/", get(show_form).post(add_new_job::<Email>))
        .layer(Extension(storage.clone()))
        .route("/metrics", get(move || ready(recorder_handle.render())));

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::debug!("listening on {}", addr);
    let http = async {
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .map_err(|e| Error::new(std::io::ErrorKind::Interrupted, e))
    };
    let monitor = async {
        Monitor::new()
            .register_with_count(2, move |_| {
                WorkerBuilder::new(storage.clone())
                    .layer(PrometheusLayer)
                    .build_fn(send_email)
            })
            .run()
            .await
    };
    futures::future::try_join(monitor, http).await.unwrap();
    Ok(())
}

fn setup_metrics_recorder() -> PrometheusHandle {
    const EXPONENTIAL_SECONDS: &[f64] = &[
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ];

    PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full("job_requests_duration_seconds".to_string()),
            EXPONENTIAL_SECONDS,
        )
        .unwrap()
        .install_recorder()
        .unwrap()
}

async fn show_form() -> Html<&'static str> {
    Html(FORM_HTML)
}

async fn add_new_job<T>(
    Form(input): Form<T>,
    Extension(mut storage): Extension<RedisStorage<T>>,
) -> impl IntoResponse
where
    T: 'static + Debug + Job + Serialize + DeserializeOwned + Unpin + Send,
{
    dbg!(&input);
    let new_job = storage.push(input).await;

    match new_job {
        Ok(()) => (
            StatusCode::CREATED,
            "Job was successfully added".to_string(),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("An Error occured {}", e),
        ),
    }
    .into_response()
}
