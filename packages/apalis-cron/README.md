# apalis-cron

A simple yet extensible library for cron-like job scheduling for rust.
Since apalis-cron is build on top of apalis which supports tower middleware, you should be able to easily add middleware such as tracing, retries, load shed, concurrency etc.

## Example

```rust
use apalis::{prelude::*, layers::retry::RetryPolicy};
use std::str::FromStr;
use apalis_cron::{CronStream, Schedule};
use chrono::{DateTime, Utc};

#[derive(Default, Debug, Clone)]
struct Reminder(DateTime<Utc>);
impl From<DateTime<Utc>> for Reminder {
   fn from(t: DateTime<Utc>) -> Self {
       Reminder(t)
   }
}
async fn handle_tick(job: Reminder, data: Data<usize>) {
    // Do something with the current tick
}

#[tokio::main]
async fn main() {
    let schedule = Schedule::from_str("@daily").unwrap();

    let worker = WorkerBuilder::new("morning-cereal")
        .retry(RetryPolicy::retries(5))
        .data(42usize)
        .backend(CronStream::new(schedule))
        .build_fn(handle_tick);

    worker.run().await;
}
```
