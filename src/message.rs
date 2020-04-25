use log::{debug, info};
use redis::Value;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::ops::{Deref, Drop};

#[derive(Debug, PartialEq)]
pub enum MessageState {
    Unacked,
    Acked,
    Rejected,
    Pushed,
}

/// Message objects that can be reconstructed from the data stored in Redis.
///
/// Implemented for all `Deserialize` objects by default by relying on Msgpack
/// decoding.
pub trait MessageDecodable
where
    Self: Sized + Send + Sync,
{
    /// Decode the given Redis value into a message
    ///
    /// In the default implementation, the string value is decoded by assuming
    /// it was encoded through the Msgpack encoding.
    fn decode_message(value: &Value) -> Result<Self, &'static str>;
}

/// Message objects that can be encoded to a string to be stored in Redis.
///
/// Implemented for all `Serialize` objects by default by encoding with Msgpack.
pub trait MessageEncodable
where
    Self: Sized + Send + Sync,
{
    /// Encode the value into a bytes array to be inserted into Redis.
    ///
    /// In the default implementation, the object is encoded with Msgpack.
    fn encode_message(&self) -> Result<Vec<u8>, &'static str>;
}

impl<T: Send + DeserializeOwned + Sync> MessageDecodable for T {
    fn decode_message(value: &Value) -> Result<T, &'static str> {
        match *value {
            Value::Data(ref v) => {
                rmp_serde::decode::from_slice(v).or(Err("failed to decode value with msgpack"))
            }
            _ => Err("can only decode from a string"),
        }
    }
}

impl<T: Serialize + Send + Sync> MessageEncodable for T {
    fn encode_message(&self) -> Result<Vec<u8>, &'static str> {
        rmp_serde::encode::to_vec(self).or(Err("failed to encode value"))
    }
}

pub struct MessageGuard<T: std::marker::Sync + Send> {
    message: T,
    payload: Vec<u8>,
    state: MessageState,
}

impl<T: std::marker::Sync + Send + 'static> MessageGuard<T> {
    pub fn new(message: T, payload: Vec<u8>) -> MessageGuard<T> {
        MessageGuard {
            message,
            payload,
            state: MessageState::Unacked,
        }
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn message(&self) -> &T {
        &self.message
    }

    /// Acknowledge the message and remove it from the *processing* queue.
    pub fn ack(&mut self) {
        self.state = MessageState::Acked;
    }

    /// Reject the message and push it from the *processing* queue to the
    /// *unack* queue.
    pub fn reject(&mut self) {
        self.state = MessageState::Rejected;
    }
}

impl<T: std::marker::Sync + Send> Deref for MessageGuard<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.message
    }
}

impl<T: std::marker::Sync + Send> Drop for MessageGuard<T> {
    fn drop(&mut self) {
        if self.state == MessageState::Unacked {
            debug!("Dropping Unacked Message");
            //let _ = tokio::run(self.reject());
        }
    }
}
