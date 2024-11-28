# apalis-cron

A simple yet extensible library for cron-like job scheduling for rust.
Since apalis-cron is build on top of apalis which supports tower middleware, you should be able to easily add middleware such as tracing, retries, load shed, concurrency etc.

## Example

```rust
use apalis::layers::retry::RetryLayer;
use apalis::layers::retry::RetryPolicy;
use tower::ServiceBuilder;
use apalis_cron::Schedule;
use std::str::FromStr;
use apalis::prelude::*;
use apalis_cron::CronStream;
use chrono::{DateTime, Utc};

#[derive(Clone)]
struct FakeService;
impl FakeService {
    fn execute(&self, item: Reminder){}
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
    let schedule = Schedule::from_str("@daily").unwrap();
    let worker = WorkerBuilder::new("morning-cereal")
        .retry(RetryPolicy::retries(5))
        .data(FakeService)
        .stream(CronStream::new(schedule).into_stream())
        .build_fn(send_reminder);
    Monitor::new()
        .register(worker)
        .run()
        .await
        .unwrap();
}
```
