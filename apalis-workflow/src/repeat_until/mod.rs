pub struct RepeatUntil<F> {
    repeat: F,
}

impl<F, Input, Res> Step<Input> for RepeatUntil<F>
where
    F: Service<Task<Input, (), ()>, Response = Option<Res>> + Send + 'static + Clone,
{
    type Response = Res;
    type Error = F::Error;
    fn register(&self, ctx: &mut Context<(), ()>) {
        // TODO
    }
}
