use crate::{error::Error, Codec};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

/// Json encoding and decoding
#[derive(Debug, Clone, Default)]
pub struct JsonCodec;

impl<T: Serialize + DeserializeOwned> Codec<T, Vec<u8>> for JsonCodec {
    type Error = Error;
    fn encode(&self, input: &T) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(input).map_err(|e| Error::SourceError(Box::new(e)))
    }

    fn decode(&self, compact: &Vec<u8>) -> Result<T, Self::Error> {
        serde_json::from_slice(compact).map_err(|e| Error::SourceError(Box::new(e)))
    }
}

impl<T: Serialize + DeserializeOwned> Codec<T, String> for JsonCodec {
    type Error = Error;
    fn encode(&self, input: &T) -> Result<String, Self::Error> {
        serde_json::to_string(input).map_err(|e| Error::SourceError(Box::new(e)))
    }

    fn decode(&self, compact: &String) -> Result<T, Self::Error> {
        serde_json::from_str(compact).map_err(|e| Error::SourceError(Box::new(e)))
    }
}

impl<T: Serialize + DeserializeOwned> Codec<T, Value> for JsonCodec {
    type Error = Error;
    fn encode(&self, input: &T) -> Result<Value, Self::Error> {
        serde_json::to_value(input).map_err(|e| Error::SourceError(Box::new(e)))
    }

    fn decode(&self, compact: &Value) -> Result<T, Self::Error> {
        serde_json::from_value(compact.clone()).map_err(|e| Error::SourceError(Box::new(e)))
    }
}
