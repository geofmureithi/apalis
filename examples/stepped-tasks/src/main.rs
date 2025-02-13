use std::time::Duration;

use apalis::prelude::*;
use apalis_redis::RedisStorage;
use serde::{Deserialize, Serialize};
use tracing::info;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct WelcomeEmail {
    user_id: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]

struct FirstWeekEmail {
    user_id: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]

struct FirstMonthEmail {
    user_id: usize,
}

async fn welcome(req: WelcomeEmail, ctx: Data<()>) -> Result<GoTo<FirstWeekEmail>, Error> {
    Ok::<_, _>(GoTo::Next(FirstWeekEmail {
        user_id: req.user_id + 1,
    }))
}

async fn first_week_email(
    req: FirstWeekEmail,
    ctx: Data<()>,
) -> Result<GoTo<FirstMonthEmail>, Error> {
    Ok::<_, _>(GoTo::Delay {
        next: FirstMonthEmail {
            user_id: req.user_id + 1,
        },
        delay: Duration::from_secs(10),
    })
}

async fn first_month_email(req: FirstMonthEmail, ctx: Data<()>) -> Result<GoTo<()>, Error> {
    Ok::<_, _>(GoTo::Done)
}

async fn produce_jobs(storage: &mut RedisStorage<StepRequest<Vec<u8>>>) {
    storage
        .push(StepRequest {
            current: 0,
            inner: serde_json::to_vec(&WelcomeEmail { user_id: 1 }).unwrap(),
        })
        .await
        .unwrap();
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    std::env::set_var("RUST_LOG", "debug,sqlx::query=error");
    tracing_subscriber::fmt::init();
    let conn = apalis_redis::connect("redis://127.0.0.1/").await.unwrap();
    let config = apalis_redis::Config::default().set_namespace("apalis_redis-with-msg-pack");

    let mut storage = RedisStorage::new_with_config(conn, config);
    produce_jobs(&mut storage).await;

    // Build steps
    let steps = StepBuilder::new()
        .step_fn(welcome)
        .step_fn(first_week_email)
        .step_fn(first_month_email);

    WorkerBuilder::new("tasty-banana")
        .data(())
        .enable_tracing()
        .concurrency(2)
        .backend(storage)
        .build_steps(steps)
        .on_event(|e| info!("{e}"))
        .run()
        .await;
    Ok(())
}
