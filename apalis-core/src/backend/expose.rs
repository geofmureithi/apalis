use crate::{
    backend::{Backend, TaskSink},
    task::{Task, status::Status},
};

const DEFAULT_PAGE_SIZE: u32 = 10;
/// Allows exposing additional functionality from the backend
pub trait Expose<Args> {}

impl<B, Args> Expose<Args> for B where
    B: Backend<Args = Args>
        + Metrics
        + ListWorkers
        + ListQueues
        + ListAllTasks
        + ListTasks<Args>
        + TaskSink<Args>
{
}

/// Allows listing all queues available in the backend
pub trait ListQueues: Backend {
    /// List all available queues in the backend
    fn list_queues(&self) -> impl Future<Output = Result<Vec<QueueInfo>, Self::Error>> + Send;
}

/// Allows listing all workers registered with the backend
pub trait ListWorkers: Backend {
    /// List all registered workers in the current queue
    fn list_workers(
        &self,
        queue: &str,
    ) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send;

    /// List all registered workers in all queues
    fn list_all_workers(
        &self,
    ) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send;
}
/// Allows listing tasks with optional filtering
pub trait ListTasks<Args>: Backend {
    /// List tasks matching the given filter in the current queue
    #[allow(clippy::type_complexity)]
    fn list_tasks(
        &self,
        queue: &str,
        filter: &Filter,
    ) -> impl Future<Output = Result<Vec<Task<Args, Self::Context, Self::IdType>>, Self::Error>> + Send;
}

/// Allows listing tasks across all queues with optional filtering
pub trait ListAllTasks: Backend {
    /// List tasks matching the given filter in all queues
    #[allow(clippy::type_complexity)]
    fn list_all_tasks(
        &self,
        filter: &Filter,
    ) -> impl Future<
        Output = Result<Vec<Task<Self::Compact, Self::Context, Self::IdType>>, Self::Error>,
    > + Send;
}

/// Allows collecting metrics from the backend
pub trait Metrics: Backend {
    /// Collects and returns global statistics from the backend
    fn global(&self) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send;

    /// Collects and returns statistics for a specific queue
    fn fetch_by_queue(
        &self,
        queue: &str,
    ) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send;
}

/// Represents information about a specific queue in the backend
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone)]
pub struct QueueInfo {
    /// Name of the queue
    pub name: String,
    /// Statistics related to the queue
    pub stats: Vec<Statistic>,
    /// List of workers associated with the queue
    pub workers: Vec<String>,
    /// Last 7 days of activity in the queue
    pub activity: Vec<usize>,
}

/// Represents a worker currently registered with the backend
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone)]
pub struct RunningWorker {
    /// Unique identifier for the worker
    pub id: String,
    /// Queue the worker is processing tasks from
    pub queue: String,
    /// Backend of the worker
    pub backend: String,
    /// Timestamp when the worker was started
    pub started_at: u64,
    /// Timestamp of the last heartbeat received from the worker
    pub last_heartbeat: u64,
    /// Layers the worker is associated with
    pub layers: String,
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
    #[must_use]
    pub fn offset(&self) -> u32 {
        (self.page - 1) * self.page_size.unwrap_or(DEFAULT_PAGE_SIZE)
    }

    /// Get the limit (page size) for the query
    #[must_use]
    pub fn limit(&self) -> u32 {
        self.page_size.unwrap_or(DEFAULT_PAGE_SIZE)
    }
}

#[cfg(feature = "serde")]
fn default_page() -> u32 {
    1
}
/// Represents an overview of the backend including queues, workers, and statistics
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone)]
pub struct Statistic {
    /// Overall statistics of the backend
    pub title: String,
    /// The statistics type
    pub stat_type: StatType,
    /// The value of the statistic
    pub value: String,
    /// The priority of the statistic (lower number means higher priority)
    pub priority: Option<u64>,
}
/// Statistics type
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone)]
pub enum StatType {
    /// Timestamp statistic
    Timestamp,
    /// Numeric statistic
    Number,
    /// Decimal statistic
    Decimal,
    /// Percentage statistic
    Percentage,
}
