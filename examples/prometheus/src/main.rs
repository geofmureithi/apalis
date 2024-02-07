//! Run with
//!
//! ```not_rust
//! cd examples && cargo run -p prometheus-example
//! ```
use anyhow::Result;
use apalis::prelude::*;
use apalis::{layers::prometheus::PrometheusLayer, redis::RedisStorage};
use axum::{
    extract::Form,
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::get,
    Extension, Router,
};
use futures::future::ready;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Debug, net::SocketAddr};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use email_service::{send_email, Email, FORM_HTML};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();
    let conn = apalis::redis::connect("redis://127.0.0.1/").await?;
    let storage = RedisStorage::new(conn);
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
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::BrokenPipe, e))
    };
    let monitor = async {
        Monitor::<TokioExecutor>::new()
            .register_with_count(2, {
                WorkerBuilder::new("tasty-banana")
                    .layer(PrometheusLayer)
                    .with_storage(storage.clone())
                    .build_fn(send_email)
            })
            .run()
            .await
    };
    let _res = futures::future::try_join(monitor, http)
        .await
        .expect("Could not start services");
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
        .expect("Could not setup Prometheus")
        .install_recorder()
        .expect("Could not install Prometheus recorder")
}

async fn show_form() -> Html<&'static str> {
    Html(FORM_HTML)
}

async fn add_new_job<T>(
    Form(input): Form<T>,
    Extension(mut storage): Extension<RedisStorage<T>>,
) -> impl IntoResponse
where
    T: 'static + Debug + Job + Serialize + DeserializeOwned + Unpin + Send + Sync,
{
    dbg!(&input);
    let new_job = storage.push(input).await;

    match new_job {
        Ok(jid) => (
            StatusCode::CREATED,
            format!("Job [{jid}] was successfully added"),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("An Error occurred {e}"),
        ),
    }
    .into_response()
}
