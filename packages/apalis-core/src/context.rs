use crate::Consumer;
use actix::Actor;
use core::marker::PhantomData;
use fnv::FnvHashMap;
use std::any::{Any, TypeId};

/// The context for consumers is represented here
pub struct JobContext<C: Consumer + Actor> {
    data: FnvHashMap<TypeId, Box<dyn Any + Sync + Send>>,
    consumer: PhantomData<C>,
}

impl<C: Consumer + Actor> JobContext<C> {
    /// Returns a JobContext for a specific consumer
    /// # Examples
    ///
    /// ```
    /// use actix::{Actor, Context};
    /// use apalis_core::{JobContext, Consumer};
    /// struct MyConsumer{}
    /// impl Actor for MyConsumer{
    ///     type Context = Context<Self>;
    /// }
    /// impl Consumer for MyConsumer{}
    /// let ctx = JobContext::<MyConsumer>::new();
    /// ```
    pub fn new() -> Self {
        JobContext {
            data: FnvHashMap::default(),
            consumer: PhantomData,
        }
    }
    /// Get data from a consumer's context
    ///
    /// # Examples
    ///
    /// ```
    /// use actix::{Actor, Context};
    /// use apalis_core::{JobContext, Consumer};
    /// struct MyConsumer{}
    /// impl Actor for MyConsumer{
    ///     type Context = Context<Self>;
    /// }
    /// impl Consumer for MyConsumer{}
    /// let mut ctx = JobContext::<MyConsumer>::new();
    /// struct DummyAddress {}
    /// ctx.insert(DummyAddress{});
    /// let _addr = ctx.data_opt::<DummyAddress>().unwrap();
    /// ```
    pub fn data_opt<D: Any + Send + Sync>(&self) -> Option<&D> {
        self.data
            .get(&TypeId::of::<D>())
            .and_then(|d| d.downcast_ref::<D>())
    }

    /// Adds data to a consumer's context
    ///
    /// # Examples
    ///
    /// ```
    /// use actix::{Actor, Context};
    /// use apalis_core::{JobContext, Consumer};
    /// struct MyConsumer{}
    /// impl Actor for MyConsumer{
    ///     type Context = Context<Self>;
    /// }
    /// impl Consumer for MyConsumer{}
    /// let mut ctx = JobContext::<MyConsumer>::new();
    /// struct DummyAddress {}
    /// ctx.insert(DummyAddress{});
    pub fn insert<D: Any + Send + Sync>(&mut self, data: D) {
        self.data.insert(TypeId::of::<D>(), Box::new(data));
    }
}
