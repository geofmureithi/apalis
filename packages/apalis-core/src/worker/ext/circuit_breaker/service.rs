use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tower_layer::Layer;
use tower_service::Service;

use crate::error::BoxDynError;

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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CircuitState {
    Closed,   // Normal operation
    Open,     // Circuit is open, rejecting requests
    HalfOpen, // Testing if service has recovered
}

#[derive(Debug)]
pub struct CircuitBreakerStats {
    pub failure_count: u32,
    pub success_count: u32,
    pub state: CircuitState,
    pub last_failure_time: Option<Instant>,
    pub half_open_calls: u32,
}

impl Default for CircuitBreakerStats {
    fn default() -> Self {
        Self {
            failure_count: 0,
            success_count: 0,
            state: CircuitState::Closed,
            last_failure_time: None,
            half_open_calls: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CircuitBreakerLayer {
    config: CircuitBreakerConfig,
}

impl CircuitBreakerLayer {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self { config }
    }
}

impl<S> Layer<S> for CircuitBreakerLayer {
    type Service = CircuitBreakerService<S>;

    fn layer(&self, service: S) -> Self::Service {
        CircuitBreakerService::new(service, self.config.clone())
    }
}

#[derive(Debug, Clone)]
pub struct CircuitBreakerService<S> {
    inner: S,
    config: CircuitBreakerConfig,
    stats: Arc<RwLock<CircuitBreakerStats>>,
}

impl<S> CircuitBreakerService<S> {
    pub fn new(service: S, config: CircuitBreakerConfig) -> Self {
        Self {
            inner: service,
            config,
            stats: Arc::new(RwLock::new(CircuitBreakerStats::default())),
        }
    }

    pub fn get_stats(&self) -> &CircuitBreakerStats {
        // let res: &CircuitBreakerStats = self.stats.try_read().map(|s|(&*s).clone()).unwrap();
        // res
        todo!()
    }

    fn should_allow_request(&self) -> bool {
        let mut stats = match self.stats.write() {
            Ok(stats) => stats,
            Err(_) => return false, // If poisoned, reject request
        };

        match stats.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if let Some(last_failure) = stats.last_failure_time {
                    if last_failure.elapsed() >= self.config.recovery_timeout {
                        stats.state = CircuitState::HalfOpen;
                        stats.half_open_calls = 0;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => {
                if stats.half_open_calls < self.config.half_open_max_calls {
                    stats.half_open_calls += 1;
                    true
                } else {
                    false
                }
            }
        }
    }

    fn record_success(&self) {
        let mut stats = match self.stats.write() {
            Ok(stats) => stats,
            Err(_) => return, // If poisoned, skip recording
        };

        stats.success_count += 1;

        if stats.state == CircuitState::HalfOpen {
            let total_calls = stats.success_count + stats.failure_count;
            let success_rate = stats.success_count as f32 / total_calls as f32;

            if success_rate >= self.config.success_threshold {
                stats.state = CircuitState::Closed;
                stats.failure_count = 0;
                stats.success_count = 0;
                stats.half_open_calls = 0;
            }
        }
    }

    fn record_failure(&self) {
        let mut stats = match self.stats.write() {
            Ok(stats) => stats,
            Err(_) => return, // If poisoned, skip recording
        };

        stats.failure_count += 1;
        stats.last_failure_time = Some(Instant::now());

        match stats.state {
            CircuitState::Closed => {
                if stats.failure_count >= self.config.failure_threshold {
                    stats.state = CircuitState::Open;
                }
            }
            CircuitState::HalfOpen => {
                stats.state = CircuitState::Open;
                stats.half_open_calls = 0;
            }
            _ => {}
        }
    }

    fn can_make_request(&self) -> bool {
        let stats = match self.stats.read() {
            Ok(stats) => stats,
            Err(_) => return false, // If poisoned, reject request
        };

        match stats.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if let Some(last_failure) = stats.last_failure_time {
                    last_failure.elapsed() >= self.config.recovery_timeout
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => stats.half_open_calls < self.config.half_open_max_calls,
        }
    }
}

#[derive(Debug)]
pub(super) enum CircuitBreakerError {
    CircuitOpen,
}

impl std::fmt::Display for CircuitBreakerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitBreakerError::CircuitOpen => write!(f, "Circuit breaker is open"),
        }
    }
}

impl std::error::Error for CircuitBreakerError {}

impl<S, Request> Service<Request> for CircuitBreakerService<S>
where
    S: Service<Request>,
    S::Future: Send + 'static,
    S::Error: Send + Sync + 'static + Into<BoxDynError>,
    Request: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxDynError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Check if circuit allows requests
        if !self.can_make_request() {
            return Poll::Pending;
        }

        // Check inner service readiness
        self.inner.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: Request) -> Self::Future {
        // Check if we should allow the request
        if !self.should_allow_request() {
            return Box::pin(async move { Err(BoxDynError::from(CircuitBreakerError::CircuitOpen)) });
        }

        let future = self.inner.call(req);
        let stats = self.stats.clone();

        Box::pin(async move {
            match future.await {
                Ok(response) => {
                    // Record success
                    if let Ok(mut s) = stats.write() {
                        s.success_count += 1;
                        if s.state == CircuitState::HalfOpen {
                            let total_calls = s.success_count + s.failure_count;
                            let success_rate = s.success_count as f32 / total_calls as f32;

                            if success_rate >= 0.5 {
                                // Use default threshold here
                                s.state = CircuitState::Closed;
                                s.failure_count = 0;
                                s.success_count = 0;
                                s.half_open_calls = 0;
                            }
                        }
                    }
                    Ok(response)
                }
                Err(e) => {
                    // Record failure
                    if let Ok(mut s) = stats.write() {
                        s.failure_count += 1;
                        s.last_failure_time = Some(Instant::now());

                        match s.state {
                            CircuitState::Closed => {
                                if s.failure_count >= 5 {
                                    // Use default threshold here
                                    s.state = CircuitState::Open;
                                }
                            }
                            CircuitState::HalfOpen => {
                                s.state = CircuitState::Open;
                                s.half_open_calls = 0;
                            }
                            _ => {}
                        }
                    }
                    Err(e.into())
                }
            }
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;

    #[derive(Clone)]
    struct MockService {
        should_fail: bool,
    }

    impl Service<()> for MockService {
        type Response = String;
        type Error = &'static str;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _: ()) -> Self::Future {
            let should_fail = self.should_fail;
            Box::pin(async move {
                if should_fail {
                    Err("Service error")
                } else {
                    Ok("success".to_string())
                }
            })
        }
    }

    #[test]
    fn test_circuit_breaker_config() {
        let config = CircuitBreakerConfig::default()
            .with_failure_threshold(10)
            .with_recovery_timeout(Duration::from_secs(30))
            .with_success_threshold(0.8)
            .with_half_open_max_calls(5);

        assert_eq!(config.failure_threshold, 10);
        assert_eq!(config.recovery_timeout, Duration::from_secs(30));
        assert_eq!(config.success_threshold, 0.8);
        assert_eq!(config.half_open_max_calls, 5);
    }

    #[test]
    fn test_circuit_states() {
        let config = CircuitBreakerConfig::default();
        let service = MockService { should_fail: false };
        let circuit_service = CircuitBreakerService::new(service, config);

        let stats = circuit_service.get_stats();
        assert_eq!(stats.state, CircuitState::Closed);
        assert_eq!(stats.failure_count, 0);
        assert_eq!(stats.success_count, 0);
    }
}
