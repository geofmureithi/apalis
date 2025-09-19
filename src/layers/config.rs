use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Configuration for worker middlewares
#[derive(Debug, Serialize, Deserialize, Clone)]
#[non_exhaustive]
pub enum MiddlewareConfig {
    /// A simple timeout middleware that cancels jobs running longer than the specified duration
    Timeout {
        /// The duration after which the job should be cancelled
        duration: Duration,
    },
    /// A load shedding middleware that drops tasks when the system is under heavy load
    LoadShed,
    /// A rate limiting middleware that limits the number of tasks processed per unit of time
    RateLimit {
        /// Maximum number of tasks to process
        num: u64,
        /// Time window for the rate limit
        per: Duration,
    },
    /// A concurrency middleware that limits the number of tasks processed concurrently
    Concurrency {
        /// Maximum number of concurrent tasks
        max: usize,
    },
    /// A buffering middleware that buffers incoming tasks up to a certain limit
    Buffer {
        /// Maximum number of tasks to buffer
        bound: usize,
    },
    /// A middleware that catches panics and prevents them from crashing the worker
    CatchPanic,
    /// A middleware that retries failed tasks up to a certain number of times
    Retries {
        /// Maximum number of retries
        max: usize,
    },
    /// A middleware that marks tasks as long-running if they exceed a certain duration
    LongRunning {
        /// Duration after which a task is considered long-running
        duration: Duration,
    },
    /// A middleware that sends acknowledgments to a specified URL after task completion
    Ack {
        /// The URL to send the acknowledgment to
        url: String,
    },
}

/// Configuration for worker middlewares
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WorkerConfig(Vec<MiddlewareConfig>);
