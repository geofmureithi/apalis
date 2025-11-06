use std::time::Duration;

/// Configuration options for the circuit breaker.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Maximum number of failures before opening the circuit
    pub failure_threshold: u32,
    /// Time to wait before attempting to close the circuit
    pub recovery_timeout: Duration,
    /// Percentage of successful requests needed to close the circuit (0.0 to 1.0)
    pub success_threshold: f32,
    /// Number of requests to allow in half-open state for testing
    pub half_open_max_calls: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            recovery_timeout: Duration::from_secs(60),
            success_threshold: 0.5,
            half_open_max_calls: 3,
        }
    }
}

// Configuration builder methods
impl CircuitBreakerConfig {
    /// Sets the failure threshold
    pub fn with_failure_threshold(mut self, threshold: u32) -> Self {
        self.failure_threshold = threshold;
        self
    }
    /// Sets the recovery timeout
    pub fn with_recovery_timeout(mut self, timeout: Duration) -> Self {
        self.recovery_timeout = timeout;
        self
    }
    /// Sets the success threshold
    pub fn with_success_threshold(mut self, threshold: f32) -> Self {
        self.success_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Sets the maximum number of calls allowed in the half-open state
    pub fn with_half_open_max_calls(mut self, max_calls: u32) -> Self {
        self.half_open_max_calls = max_calls;
        self
    }
}
