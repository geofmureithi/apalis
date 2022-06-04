//! Run with
//!
//! ```not_rust
//! cd examples && cargo run -p axum-example
//! ```

use apalis::{
    layers::TraceLayer, redis::RedisStorage, Job, Monitor, Storage, WorkerBuilder, WorkerFactoryFn,
};
use axum::{
    extract::Form,
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::get,
    Extension, Router,
};
use serde::{de::DeserializeOwned, Serialize};
use std::{fmt::Debug, io::Error, net::SocketAddr};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use email_service::{send_email, Email, FORM_HTML};

async fn show_form() -> Html<&'static str> {
    Html(FORM_HTML)
}

async fn add_new_job<T>(
    Form(input): Form<T>,
    Extension(mut storage): Extension<RedisStorage<T>>,
) -> impl IntoResponse
where
    T: 'static + Debug + Job + Serialize + DeserializeOwned,
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
    let app = Router::new()
        .route("/", get(show_form).post(add_new_job::<Email>))
        .layer(Extension(storage.clone()));

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::debug!("listening on {}", addr);
    // run it with hyper
    let http = async {
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .map_err(|e| Error::new(std::io::ErrorKind::Interrupted, e))
    };
    let monitor = async {
        let monitor = Monitor::new()
            .register_with_count(2, move |_| {
                WorkerBuilder::new(storage.clone())
                    .layer(TraceLayer::new())
                    .build_fn(send_email)
            })
            .run()
            .await;
        Ok(monitor)
    };
    let _res = futures::future::try_join(http, monitor)
        .await
        .expect("Could not start services");
    Ok(())
}
