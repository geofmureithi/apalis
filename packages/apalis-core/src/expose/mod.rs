use std::collections::HashMap;

use crate::error::JobError;
use crate::request::JobRequest;
use crate::request::JobState;
use crate::worker::WorkerId;
use chrono::DateTime;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
/// A serializable version of a worker.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExposedWorker {
    /// The Worker's Id
    worker_id: WorkerId,
    /// Target for the worker, useful for display and filtering
    /// uses [std::any::type_name]
    job_type: String,
    /// The type of job stream
    source: String,
    /// The layers that were loaded for worker. uses [std::any::type_name]
    layers: String,
    /// The last time the worker was seen. [Storage] has keep alive.
    last_seen: DateTime<Utc>,
}

impl ExposedWorker {
    /// Build a worker representation for serialization
    pub fn new<S, T>(worker_id: WorkerId, layers: String, last_seen: DateTime<Utc>) -> Self {
        ExposedWorker {
            worker_id,
            job_type: std::any::type_name::<T>().to_string(),
            source: std::any::type_name::<S>().to_string(),
            layers,
            last_seen,
        }
    }
}

/// Counts of different job states
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct JobStateCount {
    #[serde(flatten)]
    counts: HashMap<JobState, u32>,
}

impl JobStateCount {
    /// Generate a new state count
    pub fn new(counts: HashMap<JobState, u32>) -> Self {
        Self { counts }
    }
}
/// JobStream extension usually useful for management via cli, web etc
#[async_trait::async_trait]
pub trait JobStreamExt<Job>
where
    Self: Sized,
{
    /// List all Workers that are working on a Job Stream
    async fn list_workers(&mut self) -> Result<Vec<ExposedWorker>, JobError>;

    /// Returns the counts of jobs in different states
    async fn counts(&mut self) -> Result<JobStateCount, JobError> {
        Ok(JobStateCount::default())
    }

    /// Fetch jobs persisted from storage
    async fn list_jobs(
        &mut self,
        status: &JobState,
        page: i32,
    ) -> Result<Vec<JobRequest<Job>>, JobError>;
}
