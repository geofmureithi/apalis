use crate::{
    backend::Backend,
    task::{Task, status::Status},
};

const DEFAULT_PAGE_SIZE: u32 = 10;

/// Allows listing all workers registered with the backend
pub trait ListWorkers<Args>: Backend<Args> {
    /// List all registered workers
    fn list_workers(&self) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send;
}
/// Allows listing tasks with optional filtering
pub trait ListTasks<Args>: Backend<Args> {
    /// List tasks matching the given filter
    fn list_tasks(
        &self,
        filter: &Filter,
    ) -> impl Future<Output = Result<Vec<Task<Args, Self::Context, Self::IdType>>, Self::Error>> + Send;
}

/// Allows collecting metrics from the backend
pub trait Metrics {
    /// The error type returned by metric operations
    type Error;
    /// Collects and returns backend stats
    fn stats(&self) -> impl Future<Output = Result<Stats, Self::Error>> + Send;
    // TODO: Add more specific metrics methods as needed
}

/// Represents a worker currently registered with the backend
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone)]
pub struct RunningWorker {
    /// Unique identifier for the worker
    pub id: String,
    /// Hostname of the worker
    pub hostname: String,
    /// Process ID of the worker
    pub pid: u32,
    /// Timestamp when the worker was started
    pub started_at: u64,
    /// Timestamp of the last heartbeat received from the worker
    pub last_heartbeat: u64,
    /// Service name the worker is associated with
    pub service: String,
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone)]
/// Filter criteria for listing tasks
pub struct Filter {
    /// Optional status to filter tasks by
    #[cfg_attr(feature = "serde", serde(default))]
    pub status: Option<Status>,
    #[cfg_attr(feature = "serde", serde(default = "default_page"))]
    /// Page number for pagination (default is 1)
    pub page: u32,
    /// Optional page size for pagination (default is 10)
    #[cfg_attr(feature = "serde", serde(default))]
    pub page_size: Option<u32>,
}

impl Filter {
    /// Calculate the offset based on the current page and page size
    pub fn offset(&self) -> u32 {
        (self.page - 1) * self.page_size.unwrap_or(DEFAULT_PAGE_SIZE)
    }

    /// Get the limit (page size) for the query
    pub fn limit(&self) -> u32 {
        self.page_size.unwrap_or(DEFAULT_PAGE_SIZE)
    }
}

#[cfg(feature = "serde")]
fn default_page() -> u32 {
    1
}

/// Represents various task statistics in the backend
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Default)]
pub struct Stats {
    /// Represents pending tasks
    pub pending: usize,
    /// Represents running tasks
    pub running: usize,
    /// Represents dead tasks
    pub dead: usize,
    /// Represents failed tasks
    pub failed: usize,
    /// Represents successful tasks
    pub success: usize,
    /// Total number of tasks
    pub total: usize,
}
