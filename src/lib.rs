mod actor;
mod consumer;
mod error;
mod message;
mod producer;
// Am I doing this right?
pub use actor::QueueActor;
pub use actor::*;
pub use consumer::Consumer;
pub use consumer::Jobs;
pub use error::TaskError;
pub use producer::Producer;
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
