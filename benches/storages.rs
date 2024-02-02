use apalis::{layers::Data, prelude::*};
use apalis_core::storage::{context::Context, job::Job, Storage};
use apalis_redis::RedisStorage;
// use apalis_sql::{mysql::MysqlStorage, postgres::PostgresStorage, sqlite::SqliteStorage};
use criterion::*;
use paste::paste;
use serde::{Deserialize, Serialize};
use std::{
    future::Future,
    time::{Duration, Instant},
};
use tokio::runtime::Runtime;
macro_rules! define_bench {
    ($name:expr, $setup:expr ) => {
        paste! {
        fn [<$name>](c: &mut Criterion) {
            let size: usize = 1000;

            let mut group = c.benchmark_group($name);
            group.sample_size(10);
            group.bench_with_input(BenchmarkId::new("consume", size), &size, |b, &s| {
                b.to_async(Runtime::new().unwrap())
                    .iter_custom(|iters| async move {
                        let mut interval = tokio::time::interval(Duration::from_millis(50));
                        let storage = { $setup };
                        let mut s1 = storage.clone();
                        tokio::spawn(async move {
                            Monitor::<TokioExecutor>::new()
                                .register_with_count(1, {
                                    let worker =
                                        WorkerBuilder::new(format!("{}-bench", $name))
                                            .source(storage)
                                            .build_fn(handle_test_job);
                                    worker
                                })
                                .run()
                                .await
                                .unwrap();
                        });

                        let start = Instant::now();
                        for _ in 0..iters {
                            for _i in 0..s {
                                let _ = s1.push(TestJob).await;
                            }
                            while s1.len().await.unwrap_or(-1) != 0 {
                                interval.tick().await;
                            }
                        }
                        start.elapsed()
                    })
            });
            group.bench_with_input(BenchmarkId::new("push", size), &size, |b, &s| {
                b.to_async(Runtime::new().unwrap()).iter(|| async move {
                    let mut storage = { $setup };
                    let start = Instant::now();
                    for _i in 0..s {
                        let _ = black_box(storage.push(TestJob).await);
                    }
                    start.elapsed()
                });
            });
        }}
    };
}

#[derive(Serialize, Deserialize, Debug)]
struct TestJob;

impl Job for TestJob {
    const NAME: &'static str = "TestJob";
}

async fn handle_test_job(_req: TestJob, _ctx: Data<Context>) -> Result<(), Error> {
    Ok(())
}

// define_bench!("sqlite_in_memory", {
//     let sqlite = SqliteStorage::connect("sqlite::memory:").await.unwrap();
//     let _ = sqlite.setup().await;
//     sqlite
// });

define_bench!("redis", {
    let redis = RedisStorage::connect(env!("REDIS_URL")).await.unwrap();
    redis
});

// define_bench!("postgres", {
//     let pg = PostgresStorage::connect(env!("POSTGRES_URL"))
//         .await
//         .unwrap();
//     let _ = pg.setup().await;
//     pg
// });
// define_bench!("mysql", {
//     let mysql = MysqlStorage::connect(env!("MYSQL_URL")).await.unwrap();
//     let _ = mysql.setup().await.unwrap();
//     mysql
// });

criterion_group!(benches, redis);
criterion_main!(benches);
