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
            group.bench_with_input(BenchmarkId::new("consume", size), &size, |b, &size| {
                b.to_async(Runtime::new().unwrap())
                    .iter(|| async move {

                        let mut storage = { $setup };
                        storage.cleanup().await;
                        let mut s = storage.clone();
                        tokio::spawn(async move {
                            for i in 0..=size {
                                let _ = s.push(TestJob(i)).await;
                            }
                        });
                        async fn handle_test_job(
                            req: TestJob,
                            size: Data<usize>,
                            wrk: Context<TokioExecutor>,
                        ) -> Result<(), Error> {
                            if req.0 == *size {
                                wrk.force_stop();
                            }
                            Ok(())
                        }
                        let start = Instant::now();
                        WorkerBuilder::new(format!("{}-bench", $name))
                            .data(size as usize)
                            .backend(storage.clone())
                            .build_fn(handle_test_job)
                            .with_executor(TokioExecutor)
                            .run()
                            .await;
                        storage.cleanup().await;
                        start.elapsed()
                    })
            });
            group.bench_with_input(BenchmarkId::new("push", size), &size, |b, &s| {
                b.to_async(Runtime::new().unwrap()).iter(|| async move {
                    let mut storage = { $setup };
                    let start = Instant::now();
                    for i in 0..s {
                        let _ = black_box(storage.push(TestJob(i)).await);
                    }
                    start.elapsed()
                });
            });
        }}
    };
}

#[derive(Serialize, Deserialize, Debug)]
struct TestJob(usize);
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
            .set_namespace("redis-bench")
            .set_buffer_size(100),
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
