use crate::storage::Storage;
use actix::Actor;

pub trait Producer<S: Storage>: Actor {}
