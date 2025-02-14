use std::{fmt::Debug, time::Duration};

use apalis::{
    layers::{retry::RetryPolicy, tracing::TraceLayer},
    prelude::*,
};
use apalis_core::codec::json::JsonCodec;
use apalis_redis::{RedisContext, RedisStorage};
use serde::{Deserialize, Serialize};
use tower::ServiceBuilder;
use tracing::info;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct WelcomeEmail {
    welcome_id: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]

struct FirstWeekEmail {
    first_user_id: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]

struct FirstMonthEmail {
    second_user_id: usize,
}

async fn welcome(req: WelcomeEmail, ctx: Data<()>) -> Result<GoTo<FirstWeekEmail>, Error> {
    Ok::<_, _>(GoTo::Next(FirstWeekEmail {
        first_user_id: req.welcome_id + 1,
    }))
}

async fn first_week_email(
    req: FirstWeekEmail,
    ctx: Data<()>,
) -> Result<GoTo<FirstMonthEmail>, Error> {
    Ok::<_, _>(GoTo::Delay {
        next: FirstMonthEmail {
            second_user_id: req.first_user_id + 1,
        },
        delay: Duration::from_secs(10),
    })
}

async fn first_month_email(
    req: FirstMonthEmail,
    ctx: Data<()>,
) -> Result<GoTo<&'static str>, Error> {
    Ok::<_, _>(GoTo::Done("Completed job successfully"))
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    std::env::set_var("RUST_LOG", "debug,sqlx::query=error");
    tracing_subscriber::fmt::init();
    let conn = apalis_redis::connect("redis://127.0.0.1/").await.unwrap();
    let config = apalis_redis::Config::default().set_namespace("apalis_redis-with-msg-pack");

    let mut storage = RedisStorage::new_with_config(conn, config);
    storage
        .start_stepped(WelcomeEmail { welcome_id: 1 })
        .await
        .unwrap();

    let welcome = ServiceBuilder::new()
        .retry(RetryPolicy::retries(5)) // welcome will specifically be retried 5 times
        .service(service_fn(welcome));
    // Build steps
    let steps = StepBuilder::new()
        .step(welcome)
        .step_fn(first_week_email)
        .step_fn(first_month_email);

    WorkerBuilder::new("tasty-banana")
        .data(())
        .enable_tracing()
        .concurrency(2)
        .backend(storage)
        .build_stepped(steps)
        .on_event(|e| info!("{e}"))
        .run()
        .await;
    Ok(())
}
