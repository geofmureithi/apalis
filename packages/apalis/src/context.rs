use crate::Consumer;
use actix::Actor;
use core::marker::PhantomData;
use fnv::FnvHashMap;
use std::any::{Any, TypeId};

pub struct JobContext<C: Consumer + Actor> {
    data: FnvHashMap<TypeId, Box<dyn Any + Sync + Send>>,
    consumer: PhantomData<C>,
}

impl<C: Consumer + Actor> JobContext<C> {
    pub fn new() -> Self {
        JobContext {
            data: FnvHashMap::default(),
            consumer: PhantomData,
        }
    }
    pub fn data_opt<D: Any + Send + Sync>(&self) -> Option<&D> {
        self.data
            .get(&TypeId::of::<D>())
            .and_then(|d| d.downcast_ref::<D>())
    }

    pub fn insert<D: Any + Send + Sync>(&mut self, data: D) {
        self.data.insert(TypeId::of::<D>(), Box::new(data));
    }
}
