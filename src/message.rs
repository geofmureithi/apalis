use redis::Value;
use serde::de::DeserializeOwned;
use serde::Serialize;
use crate::consumer::Job;
/// Message objects that can be reconstructed from the data stored in Redis.
///
/// Implemented for all `Deserialize` objects by default by relying on Msgpack
/// decoding.
pub trait MessageDecodable
where
    Self: Sized,
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
    Self: Sized,
{
    /// Encode the value into a bytes array to be inserted into Redis.
    ///
    /// In the default implementation, the object is encoded with Msgpack.
    fn encode_message(&self) -> Result<Vec<u8>, &'static str>;
}

impl<T: DeserializeOwned> MessageDecodable for Job<T> {
    fn decode_message(value: &Value) -> Result<Job<T>, &'static str> {
        match *value {
            Value::Data(ref v) => {
                rmp_serde::decode::from_slice(v).or(Err("failed to decode value with msgpack"))
            }
            _ => Err("can only decode from a string"),
        }
    }
}

impl<T: Serialize> MessageEncodable for T {
    fn encode_message(&self) -> Result<Vec<u8>, &'static str> {
        rmp_serde::encode::to_vec(self).or(Err("failed to encode value"))
    }
}
