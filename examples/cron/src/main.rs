use apalis::prelude::*;

use apalis_cron::CronStream;
use apalis_cron::Schedule;
use chrono::{DateTime, Utc};
use std::str::FromStr;
use std::time::Duration;
// use std::time::Duration;
use tower::load_shed::LoadShedLayer;

#[derive(Clone)]
struct FakeService;
impl FakeService {
    fn execute(&self, item: Reminder) {
        dbg!(&item.0);
    }
}

#[derive(Default, Debug, Clone)]
struct Reminder(DateTime<Utc>);
impl From<DateTime<Utc>> for Reminder {
    fn from(t: DateTime<Utc>) -> Self {
        Reminder(t)
    }
}
async fn send_reminder(job: Reminder, svc: Data<FakeService>) {
    svc.execute(job);
}

#[tokio::main]
async fn main() {
    let schedule = Schedule::from_str("1/1 * * * * *").unwrap();
    let worker = WorkerBuilder::new("morning-cereal")
        .enable_tracing()
        .layer(LoadShedLayer::new()) // Important when you have layers that block the service
        .rate_limit(1, Duration::from_secs(2))
        .data(FakeService)
        .backend(CronStream::new(schedule))
        .build_fn(send_reminder);
    Monitor::new().register(worker).run().await.unwrap();
}
