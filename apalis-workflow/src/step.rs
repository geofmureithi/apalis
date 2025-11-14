use apalis_core::{backend::BackendExt, error::BoxDynError};

use crate::router::WorkflowRouter;

/// A layer to wrap a step
pub trait Layer<S> {
    /// The resulting step type after layering.
    type Step;
    /// Wrap the given step with this layer.
    fn layer(&self, step: S) -> Self::Step;
}

/// A workflow step
///
/// A single unit of work in a workflow pipeline.
pub trait Step<Input, B>
where
    B: BackendExt,
{
    /// The response type produced by the step.
    type Response;
    /// The error type produced by the step.
    type Error;

    /// Register the step with the workflow router.
    fn register(&mut self, router: &mut WorkflowRouter<B>) -> Result<(), BoxDynError>;
}

/// A no-op identity layer.
#[derive(Clone, Debug)]
pub struct Identity;

impl<S> Layer<S> for Identity {
    type Step = S;

    fn layer(&self, step: S) -> Self::Step {
        step
    }
}

/// Two steps chained together.
#[derive(Clone, Debug)]
pub struct Stack<Inner, Outer> {
    inner: Inner,
    outer: Outer,
}
impl<Inner, Outer> Stack<Inner, Outer> {
    /// Create a new `Stack`.
    pub const fn new(inner: Inner, outer: Outer) -> Self {
        Stack { inner, outer }
    }
}

impl<S, Inner, Outer> Layer<S> for Stack<Inner, Outer>
where
    Inner: Layer<S>,
    Outer: Layer<Inner::Step>,
{
    type Step = Outer::Step;

    fn layer(&self, service: S) -> Self::Step {
        let inner = self.inner.layer(service);

        self.outer.layer(inner)
    }
}
