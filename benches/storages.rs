use apalis::prelude::*;

use apalis::redis::RedisStorage;
use apalis::{
    mysql::{MySqlPool, MysqlStorage},
    postgres::{PgPool, PostgresStorage},
    sqlite::{SqlitePool, SqliteStorage},
};
use criterion::*;
use paste::paste;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
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

async fn handle_test_job(_req: TestJob) -> Result<(), Error> {
    Ok(())
}

define_bench!("sqlite_in_memory", {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    let _ = SqliteStorage::setup(&pool).await;
    SqliteStorage::new(pool)
});

define_bench!("redis", {
    let conn = apalis::redis::connect(env!("REDIS_URL")).await.unwrap();
    let redis = RedisStorage::new(conn);
    redis
});

define_bench!("postgres", {
    let pool = PgPool::connect(env!("POSTGRES_URL")).await.unwrap();
    let _ = PostgresStorage::setup(&pool).await.unwrap();
    PostgresStorage::new(pool)
});
// TODO: See why it no complete.
// define_bench!("mysql", {
//     let pool = MySqlPool::connect(env!("MYSQL_URL")).await.unwrap();
//     let _ = MysqlStorage::setup(&pool).await.unwrap();
//     MysqlStorage::new(pool)
// });

criterion_group!(benches, sqlite_in_memory, redis, postgres);
criterion_main!(benches);
