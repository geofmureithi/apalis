use std::marker::PhantomData;

use crate::{backend::Backend, memory::MemoryStorage};

pub trait MakeShared<Req> {
    type Backend;
    /// The Config for the backend
    type Config;
    /// The error returned if the backend cant be shared
    type MakeError;

    /// Returns the backend to be shared
    fn make_shared(&mut self) -> Result<Self::Backend, Self::MakeError>;

    /// Returns the backend with config
    fn make_shared_with_config(&mut self, config: Self::Config)
        -> Result<Self::Backend, Self::MakeError>;
}

// pub struct JsonStorage {
//     job
// }
