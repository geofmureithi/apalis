use std::time::Duration;

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
    pub fn with_failure_threshold(mut self, threshold: u32) -> Self {
        self.failure_threshold = threshold;
        self
    }

    pub fn with_recovery_timeout(mut self, timeout: Duration) -> Self {
        self.recovery_timeout = timeout;
        self
    }

    pub fn with_success_threshold(mut self, threshold: f32) -> Self {
        self.success_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    pub fn with_half_open_max_calls(mut self, max_calls: u32) -> Self {
        self.half_open_max_calls = max_calls;
        self
    }
}
