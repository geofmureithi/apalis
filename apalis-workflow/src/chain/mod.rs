// pub struct Chain<SubFlow> {
//     sub_flow: SubFlow,
// }

// impl<L, Input, Output> Step<Input> for Chain<Workflow<Input, Output, L>> {
//     type Response = Output;
//     type Error = Infallible;
//     fn register(&self, ctx: &mut Context<(), ()>) {
//         // TODO
//     }
// }

// impl<Input> Step<Input> for Chain<DagExecutor> {
//     type Response = ();
//     type Error = Infallible;
//     fn register(&self, ctx: &mut StepRouter<()>) {
//         // TODO
//     }
// }
