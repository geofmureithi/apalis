use tower_layer::{Layer, Stack};

use crate::{
    backend::Backend,
    worker::{
        builder::WorkerBuilder,
        ext::circuit_breaker::service::{CircuitBreakerConfig, CircuitBreakerLayer},
    },
};

mod service;

pub trait CircuitBreaker<Args, Ctx, Source, Middleware>: Sized {
    /// Allows the worker to break the circuit in case of failures
    /// Uses default configuration
    fn break_circuit(
        self,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<CircuitBreakerLayer, Middleware>> {
        self.break_circuit_with(CircuitBreakerConfig::default())
    }

    /// Allows the worker to break the circuit w in case of failures
    /// Allows customizing the CircuitBreakerConfig
    fn break_circuit_with(
        self,
        cfg: CircuitBreakerConfig,
    ) -> WorkerBuilder<Args, Ctx, Source, Stack<CircuitBreakerLayer, Middleware>>;
}

impl<Args, P, M, Ctx> CircuitBreaker<Args, Ctx, P, M> for WorkerBuilder<Args, Ctx, P, M>
where
    P: Backend<Args, Ctx>,
    M: Layer<CircuitBreakerLayer>,
{
    fn break_circuit_with(
        self,
        config: CircuitBreakerConfig,
    ) -> WorkerBuilder<Args, Ctx, P, Stack<CircuitBreakerLayer, M>> {
        let this = self.layer(CircuitBreakerLayer::new(config));
        WorkerBuilder {
            name: this.name,
            request: this.request,
            layer: this.layer,
            source: this.source,
            shutdown: this.shutdown,
            event_handler: this.event_handler,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tower::limit::ConcurrencyLimitLayer;

    use crate::{
        backend::{memory::MemoryStorage, BackendWithSink, TaskSink},
        error::BoxDynError,
        worker::{
            builder::WorkerBuilder,
            context::WorkerContext,
            ext::{circuit_breaker::CircuitBreaker, event_listener::EventListenerExt},
        },
    };

    use super::*;

    const ITEMS: u32 = 10;

    #[tokio::test]
    async fn basic_worker() {
        let in_memory = MemoryStorage::new();
        let mut sink = in_memory.sink();
        for i in 0..ITEMS {
            sink.push(i).await.unwrap();
        }

        async fn task(task: u32, ctx: WorkerContext) -> Result<(), BoxDynError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if task == ITEMS - 1 {
                ctx.stop().unwrap();
                return Err("Worker stopped!")?;
            }
            if (task == 8) {
                    panic!("{task}");
                }
            if task % 3 == 0 {
                return Ok(());
            } else {
                return Err("Expected Error stopped!")?;
            }
        }

        let config = CircuitBreakerConfig::default()
            .with_failure_threshold(1)
            .with_recovery_timeout(Duration::from_secs(1))
            .with_success_threshold(0.5)
            .with_half_open_max_calls(1);

        let worker = WorkerBuilder::new("rango-tango")
            .backend(in_memory)
            .break_circuit_with(config)
            .layer(ConcurrencyLimitLayer::new(3))
            .on_event(|_ctx, ev| {
                println!("On Event = {:?}", ev);
            })
            .build(task);
        worker.run().await.unwrap();
    }
}
