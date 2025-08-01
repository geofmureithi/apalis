
pub trait Codec<T>: Encoder<T> + Decoder<T> {
    type Error;
    type Compact;
}

pub trait Encoder<T> {
    type Error;
    type Compact;

    fn encode(val: &T) -> Result<Self::Compact, Self::Error>;
}

#[derive(Debug, Clone, Default)]
pub struct CloneOpCodec {}

impl<T: Clone> Encoder<T> for CloneOpCodec {
    type Error = String;
    type Compact = T;
    fn encode(val: &T) -> Result<Self::Compact, Self::Error> {
        Ok(val.clone())
    }
}
pub trait Decoder<T> {
    type Error;
    type Compact;

    fn decode(val: &Self::Compact) -> Result<T, Self::Error>;
}

impl<T: Clone> Decoder<T> for CloneOpCodec {
    type Error = String;
    type Compact = T;
    fn decode(t: &Self::Compact) -> Result<T, Self::Error> {
        Ok(t.clone())
    }
}

/// Encoding for tasks using json
#[cfg(feature = "json")]
pub mod json {
    use std::marker::PhantomData;

    use serde::{Deserialize, Serialize};
    use serde_json::Value;

    use super::{Decoder, Encoder};

    /// Json encoding and decoding
    #[derive(Debug, Clone, Default)]
    pub struct JsonCodec<Output> {
        _o: PhantomData<Output>,
    }

    impl<T: Serialize> Encoder<T> for JsonCodec<Vec<u8>> {
        type Compact = Vec<u8>;
        type Error = serde_json::Error;
        fn encode(input: &T) -> Result<Vec<u8>, Self::Error> {
            serde_json::to_vec(input)
        }
    }

    impl<T> Decoder<T> for JsonCodec<Vec<u8>>
    where
        T: for<'de> Deserialize<'de>,
    {
        type Compact = Vec<u8>;
        type Error = serde_json::Error;
        fn decode(compact: &Vec<u8>) -> Result<T, Self::Error> {
            serde_json::from_slice(&compact)
        }
    }

    impl<T: Serialize> Encoder<T> for JsonCodec<String> {
        type Compact = String;
        type Error = serde_json::Error;
        fn encode(input: &T) -> Result<String, Self::Error> {
            serde_json::to_string(input)
        }
    }

    impl<T> Decoder<T> for JsonCodec<String>
    where
        T: for<'de> Deserialize<'de>,
    {
        type Compact = String;
        type Error = serde_json::Error;
        fn decode(compact: &String) -> Result<T, Self::Error> {
            serde_json::from_str(&compact)
        }
    }

    impl<T: Serialize> Encoder<T> for JsonCodec<Value> {
        type Compact = Value;
        type Error = serde_json::Error;
        fn encode(input: &T) -> Result<Value, Self::Error> {
            serde_json::to_value(input)
        }
    }

    impl<T> Decoder<T> for JsonCodec<Value>
    where
        T: for<'de> Deserialize<'de>,
    {
        type Compact = Value;
        type Error = serde_json::Error;
        fn decode(compact: &Value) -> Result<T, Self::Error> {
            serde_json::from_value(compact.clone())
        }
    }
}
