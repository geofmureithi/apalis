// Imports /////////////////////////////////////////////////////////////////////
use apalis::prelude::*;
use apalis_sql::{
    postgres::{PgListen, PgPool, PostgresStorage},
    Config,
};
use chrono::{DateTime, Utc};
use serde_json::Value;

// Constants ///////////////////////////////////////////////////////////////////
const DATABASE_URL: &str = "postgres://pg:pw@localhost/apalis";

// Main ////////////////////////////////////////////////////////////////////////
#[tokio::main]
async fn main() -> Result<(), Error> {
    // Connect to database by creating a `sqlx` connection pool for Apalis.
    let pool = PgPool::connect(DATABASE_URL).await?;

    // Run Apalis migrations
    PostgresStorage::setup(&pool).await?;

    // Define storage for stepped workflow.
    let mut wf1_storage = PostgresStorage::new_with_config(
        pool.clone(),
        Config::new("apalis::postgres-stepped::workflow_1"),
    );

    // Define steps of workflow_1.
    let wf1_steps = StepBuilder::new().step_fn(wf1::step_1).step_fn(wf1::step_2);

    // Define worker builder for workflow_1
    let wf1_worker_builder = WorkerBuilder::new("wf1_worker_builder")
        .backend(wf1_storage.clone())
        .build_stepped(wf1_steps);

    // Produce jobs in workflow_1
    wf1::produce_jobs(wf1_storage.clone()).await?;

    // Create database listener, subscribe the storages of all workflows to
    // this listener and start the listener in a separate tokio task.
    let mut listener = PgListen::new(pool).await?;

    listener.subscribe_with(&mut wf1_storage);

    tokio::spawn(async move {
        let res = listener.listen().await;
        println!("Database listener stopped: {res:?}");
    });

    // Create Apalis monitor, register workflows and run it.
    Monitor::new().register(wf1_worker_builder).run().await?;

    Ok(())
}

// Workflow 1 //////////////////////////////////////////////////////////////////
mod wf1 {
    use super::*;

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub struct Step1 {}

    pub async fn step_1(_req: Step1) -> Result<GoTo<Step2>, Error> {
        let now = Utc::now();
        println!("[{}][WF 1] step 1", now);

        Ok(GoTo::Delay {
            next: Step2 { start: now },
            delay: std::time::Duration::from_secs(4),
        })
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub struct Step2 {
        start: DateTime<Utc>,
    }

    pub async fn step_2(req: Step2) -> Result<GoTo<()>, Error> {
        let now = Utc::now();
        let elapsed = (now - req.start).num_milliseconds();
        println!("[{}][WF 1] step 2 (elapsed {}ms)", now, elapsed);

        Ok(GoTo::Done(()))
    }

    pub async fn produce_jobs(
        mut wf1_storage: PostgresStorage<StepRequest<Value>>,
    ) -> Result<(), Error> {
        let _ = wf1_storage.start_stepped(Step1 {}).await?;
        Ok(())
    }
}

// Error ///////////////////////////////////////////////////////////////////////
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Apalis error: {0}")]
    ApalisSqlx(#[from] apalis_sql::sqlx::Error),

    #[error("Apalis IO: {0}")]
    ApalisIo(#[from] std::io::Error),

    #[error("Apalis step error: {0}")]
    StepError(#[from] apalis::prelude::StepError),
}

////////////////////////////////////////////////////////////////////////////////
