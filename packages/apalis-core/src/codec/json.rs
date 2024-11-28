use std::marker::PhantomData;

use crate::codec::Codec;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Json encoding and decoding
#[derive(Debug, Clone, Default)]
pub struct JsonCodec<Output> {
    _o: PhantomData<Output>,
}

impl Codec for JsonCodec<Vec<u8>> {
    type Compact = Vec<u8>;
    type Error = serde_json::Error;
    fn encode<T: Serialize>(input: T) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(&input)
    }

    fn decode<O>(compact: Vec<u8>) -> Result<O, Self::Error>
    where
        O: for<'de> Deserialize<'de>,
    {
        serde_json::from_slice(&compact)
    }
}

impl Codec for JsonCodec<String> {
    type Compact = String;
    type Error = serde_json::Error;
    fn encode<T: Serialize>(input: T) -> Result<String, Self::Error> {
        serde_json::to_string(&input)
    }

    fn decode<O>(compact: String) -> Result<O, Self::Error>
    where
        O: for<'de> Deserialize<'de>,
    {
        serde_json::from_str(&compact)
    }
}

impl Codec for JsonCodec<Value> {
    type Compact = Value;
    type Error = serde_json::Error;
    fn encode<T: Serialize>(input: T) -> Result<Value, Self::Error> {
        serde_json::to_value(input)
    }

    fn decode<O>(compact: Value) -> Result<O, Self::Error>
    where
        O: for<'de> Deserialize<'de>,
    {
        serde_json::from_value(compact)
    }
}
