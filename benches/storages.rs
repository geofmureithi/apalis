use apalis::prelude::*;

use apalis::redis::RedisStorage;
use apalis::{
    mysql::{MySqlPool, MysqlStorage},
    postgres::{PgPool, PostgresStorage},
    sqlite::{SqlitePool, SqliteStorage},
};
use criterion::*;
use futures::Future;
use paste::paste;
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
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
                        let mut interval = tokio::time::interval(Duration::from_millis(100));
                        let storage = { $setup };
                        let mut s1 = storage.clone();
                        let counter = Counter::default();
                        let c = counter.clone();
                        tokio::spawn(async move {
                            Monitor::<TokioExecutor>::new()
                                .register({
                                    let worker =
                                        WorkerBuilder::new(format!("{}-bench", $name))
                                            .data(c)
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
                            counter.0.store(0, Ordering::Relaxed);
                        }
                        let elapsed = start.elapsed();
                        s1.cleanup().await;
                        elapsed
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
#[derive(Debug, Default, Clone)]
struct Counter(Arc<AtomicUsize>);

async fn handle_test_job(_req: TestJob, counter: Data<Counter>) -> Result<(), Error> {
    counter.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}

trait CleanUp {
    fn cleanup(&mut self) -> impl Future<Output = ()> + Send;
}

impl CleanUp for SqliteStorage<TestJob> {
    async fn cleanup(&mut self) {
        let pool = self.pool();
        let query = "DELETE FROM Jobs; DELETE from Workers;";
        sqlx::query(query).execute(pool).await.unwrap();
    }
}

impl CleanUp for PostgresStorage<TestJob> {
    async fn cleanup(&mut self) {
        let pool = self.pool();
        let query = "DELETE FROM apalis.jobs;";
        sqlx::query(query).execute(pool).await.unwrap();
        let query = "DELETE from apalis.workers;";
        sqlx::query(query).execute(pool).await.unwrap();
    }
}

impl CleanUp for MysqlStorage<TestJob> {
    async fn cleanup(&mut self) {
        let pool = self.pool();
        let query = "DELETE FROM jobs; DELETE from workers;";
        sqlx::query(query).execute(pool).await.unwrap();
    }
}

impl CleanUp for RedisStorage<TestJob> {
    async fn cleanup(&mut self) {
        let mut conn = self.get_connection().clone();
        let _resp: String = redis::cmd("FLUSHDB")
            .query_async(&mut conn)
            .await
            .expect("failed to Flushdb");
    }
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

define_bench!("mysql", {
    let pool = MySqlPool::connect(env!("MYSQL_URL")).await.unwrap();
    let _ = MysqlStorage::setup(&pool).await.unwrap();
    MysqlStorage::new(pool)
});

criterion_group!(benches, sqlite_in_memory, redis, postgres);
criterion_main!(benches);
