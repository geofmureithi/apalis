use apalis::prelude::*;
use apalis_redis::RedisStorage;
use apalis_sql::mysql::MysqlStorage;
use apalis_sql::postgres::PostgresStorage;
use apalis_sql::sqlite::SqliteStorage;
use apalis_sql::Config;
use criterion::*;
use futures::Future;
use paste::paste;
use serde::{Deserialize, Serialize};
use sqlx::MySqlPool;
use sqlx::PgPool;
use sqlx::SqlitePool;
use std::sync::atomic::AtomicUsize;

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::runtime::Runtime;
macro_rules! define_bench {
    ($name:expr, $setup:expr ) => {
        paste! {
        fn [<$name>](c: &mut Criterion) {
            let size: usize = 1000;

            let mut group = c.benchmark_group($name);
            group.sample_size(10);
            group.bench_function(BenchmarkId::new("consume", size),|b| {
                b.to_async(Runtime::new().unwrap())
                    .iter(|| async move {
                        let mut storage = { $setup };
                        let mut s = storage.clone();
                        let counter = Counter::default();
                        tokio::spawn(async move {
                            for _i in 0..1000 {
                                let _ = s.push(TestJob).await;
                            }
                        });
                        WorkerBuilder::new(format!("{}-bench", $name))
                            .data(counter)
                            .backend(storage.clone())
                            .build_fn(handle_test_job)
                            .with_executor(TokioExecutor)
                            .run()
                            .await;
                        storage.cleanup().await;
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

async fn handle_test_job(
    _req: TestJob,
    counter: Data<Counter>,
    wrk: Context<TokioExecutor>,
) -> Result<(), Error> {
    let value = counter.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if value == 999 {
        wrk.force_stop();
    }
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
    SqliteStorage::new_with_config(
        pool,
        Config::default()
            .set_buffer_size(100)
            .set_poll_interval(Duration::from_millis(50)),
    )
});

define_bench!("redis", {
    let conn = apalis_redis::connect(env!("REDIS_URL")).await.unwrap();
    let redis = RedisStorage::new_with_config(
        conn,
        apalis_redis::Config::default()
            .set_buffer_size(100)
            .set_poll_interval(Duration::from_millis(50)),
    );
    redis
});

define_bench!("postgres", {
    let pool = PgPool::connect(env!("POSTGRES_URL")).await.unwrap();
    let _ = PostgresStorage::setup(&pool).await.unwrap();
    PostgresStorage::new_with_config(
        pool,
        Config::new("postgres:bench")
            .set_buffer_size(100)
            .set_poll_interval(Duration::from_millis(50)),
    )
});

define_bench!("mysql", {
    let pool = MySqlPool::connect(env!("MYSQL_URL")).await.unwrap();
    let _ = MysqlStorage::setup(&pool).await.unwrap();
    MysqlStorage::new_with_config(
        pool,
        Config::new("mysql:bench")
            .set_buffer_size(100)
            .set_poll_interval(Duration::from_millis(50)),
    )
});

criterion_group!(benches, sqlite_in_memory, redis, postgres, mysql);
criterion_main!(benches);
