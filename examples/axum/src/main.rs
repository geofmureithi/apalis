//! Run with
//!
//! ```not_rust
//! cd examples && cargo run -p axum-example
//! ```
use anyhow::Result;

use apalis::prelude::*;
use apalis_redis::RedisStorage;
use axum::{
    extract::Form,
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::get,
    Extension, Router,
};
use serde::{de::DeserializeOwned, Serialize};
use std::io;
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
    T: 'static + Debug + Serialize + DeserializeOwned + Send + Sync + Unpin,
{
    dbg!(&input);
    let new_job = storage.push(input).await;

    match new_job {
        Ok(ctx) => (
            StatusCode::CREATED,
            format!("Job [{ctx:?}] was successfully added"),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("An Error occurred {e}"),
        ),
    }
    .into_response()
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();
    let redis_url = std::env::var("REDIS_URL").expect("Missing env variable REDIS_URL");
    let conn = apalis_redis::connect(redis_url)
        .await
        .expect("Could not connect");
    let storage = RedisStorage::new(conn);
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
        Monitor::new()
            .register({
                WorkerBuilder::new("tasty-pear")
                    .enable_tracing()
                    .backend(storage.clone())
                    .build_fn(send_email)
            })
            .run()
            .await
            .unwrap();
        Ok::<(), io::Error>(())
    };
    let _res = tokio::join!(http, monitor);
    Ok(())
}
