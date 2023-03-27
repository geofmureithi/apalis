use std::time::{Duration, Instant};

use apalis::prelude::*;
use apalis_sql::sqlite::SqliteStorage;
use criterion::*;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

#[derive(Serialize, Deserialize, Debug)]
struct TestJob;

impl Job for TestJob {
    const NAME: &'static str = "TestJob";
}

async fn handle_test_job(_req: TestJob, _ctx: JobContext) -> Result<(), JobError> {
    Ok(())
}

fn bench(c: &mut Criterion) {
    // c.bench_function("redis", move |b| {
    //     b.to_async(Runtime::new().unwrap())
    //         .iter_custom(|iters| async move {
    //             let mut storage = RedisStorage::new("redis://127.0.0.1/").await.unwrap();
    //             let start = Instant::now();
    //             for _i in 0..iters {
    //                 storage.push(TestJob).await.unwrap();
    //             }
    //             start.elapsed()
    //         })
    // });
    // c.bench_function("sqlite", move |b| {
    //     b.to_async(Runtime::new().unwrap())
    //         .iter_custom(|iters| async move {
    //             let mut sqlite = SqliteStorage::new("sqlite::memory:").await.unwrap();
    //             sqlite.setup().await;
    //             let start = Instant::now();
    //             for _i in 0..iters {
    //                 sqlite.push(TestJob).await.unwrap();
    //             }
    //             let len = sqlite.len().await.unwrap();
    //             assert_eq!(len as u64, iters);
    //             start.elapsed()
    //         })
    // });

    let mut group = c.benchmark_group("sample-size-example");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("sqlite_consume", move |b| {
        b.to_async(Runtime::new().unwrap())
            .iter_custom(|_iters| async move {
                let mut interval = tokio::time::interval(Duration::from_millis(10));
                let mut sqlite = SqliteStorage::connect("sqlite::memory:").await.unwrap();
                let _ = sqlite.setup().await;
                for _i in 0..100 {
                    let _ = sqlite.push(TestJob).await;
                }
                let _addr = WorkerBuilder::new("sqlite-bench")
                    .with_storage(sqlite.clone())
                    .build(job_fn(handle_test_job));

                let start = Instant::now();
                while sqlite.len().await.unwrap_or(-1) != 0 {
                    interval.tick().await;
                }

                start.elapsed()
            })
    });
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
