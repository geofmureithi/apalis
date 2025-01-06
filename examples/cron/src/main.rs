use apalis::prelude::*;

use apalis_cron::CronContext;
use apalis_cron::CronStream;
use apalis_cron::Schedule;
use chrono::Local;
use std::str::FromStr;
use std::time::Duration;
use tower::load_shed::LoadShedLayer;

#[derive(Debug, Default)]
struct Reminder;

async fn send_reminder(_job: Reminder, ctx: CronContext<Local>) {
    println!("Running cronjob for timestamp: {}", ctx.get_timestamp())
    // Do something
}

#[tokio::main]
async fn main() {
    let schedule = Schedule::from_str("1/1 * * * * *").unwrap();
    let worker = WorkerBuilder::new("morning-cereal")
        .enable_tracing()
        .layer(LoadShedLayer::new()) // Important when you have layers that block the service
        .rate_limit(1, Duration::from_secs(2))
        .backend(CronStream::new_with_timezone(schedule, Local))
        .build_fn(send_reminder);
    worker.run().await;
}
