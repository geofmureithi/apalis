use actix::Actor;

/// Represents an actor that can push jobs
pub trait Producer: Actor {}
