//! Utilities for encoding and decoding task arguments and results
//! 
//! # Overview
//! 
//! The `Codec` trait allows for converting values
//! between a type `T` and a more compact or transport-friendly representation.
//! This is particularly useful for serializing/deserializing, compressing/expanding,
//! or otherwise encoding/decoding values in a custom format.
//! 
//! The module includes several implementations of the `Codec` trait, such as `IdentityCodec`
//! and `NoopCodec`, as well as a JSON codec when the `json` feature is enabled.
/// A trait for converting values between a type `T` and a more compact or
/// transport-friendly representation for a [`Backend`]. Examples include json
/// and bytes.
///
/// This is useful when you need to serialize/deserialize, compress/expand,
/// or otherwise encode/decode values in a custom format.
///
/// By default, a backend doesn't care about the specific type implementing [`Codec`]
/// but rather the [`Codec::Compact`] type. This means if it can accept bytes, you
/// can use familiar crates such as bincode and rkyv
///
/// # Type Parameters
/// - `T`: The type of value being encoded/decoded.
pub trait Codec<T> {
    /// The error type returned if encoding or decoding fails.
    type Error;

    /// The compact or encoded representation of `T`.
    ///
    /// This could be a primitive type, a byte buffer, or any other
    /// representation that is more efficient to store or transmit.
    type Compact;

    /// Encode a value of type `T` into its compact representation.
    ///
    /// # Errors
    /// Returns [`Self::Error`] if the value cannot be encoded.
    fn encode(val: &T) -> Result<Self::Compact, Self::Error>;

    /// Decode a compact representation back into a value of type `T`.
    ///
    /// # Errors
    /// Returns [`Self::Error`] if the compact representation cannot
    /// be decoded into a valid `T`.
    fn decode(val: &Self::Compact) -> Result<T, Self::Error>;
}

/// A codec that performs no transformation, returning the input value as-is.
#[derive(Debug, Clone, Default)]
pub struct IdentityCodec;   

impl<T> Codec<T> for IdentityCodec
where
    T: Clone,
{
    type Compact = T;
    type Error = std::convert::Infallible;

    fn encode(val: &T) -> Result<Self::Compact, Self::Error> {
        Ok(val.clone())
    }

    fn decode(val: &Self::Compact) -> Result<T, Self::Error> {
        Ok(val.clone())
    }
}

/// A codec that should never be used. This is primarily a placeholder
#[derive(Debug, Clone, Default)]
pub struct NoopCodec;

impl<T> Codec<T> for NoopCodec {
    type Compact = T;
    type Error = std::convert::Infallible;

    fn encode(_val: &T) -> Result<Self::Compact, Self::Error> {
        unreachable!("NoopCodec should never be used for encoding, please use a real codec")
    }

    fn decode(_val: &Self::Compact) -> Result<T, Self::Error> {
        unreachable!("NoopCodec should never be used for decoding, please use a real codec")
    }
}

/// Encoding for tasks using json
#[cfg(feature = "json")]
pub mod json {
    use std::marker::PhantomData;

    use serde::{de::DeserializeOwned, Serialize};
    use serde_json::Value;

    use super::Codec;

    /// Json encoding and decoding
    #[derive(Debug, Clone, Default)]
    pub struct JsonCodec<Output> {
        _o: PhantomData<Output>,
    }

    impl<T: Serialize + DeserializeOwned> Codec<T> for JsonCodec<Vec<u8>> {
        type Compact = Vec<u8>;
        type Error = serde_json::Error;
        fn encode(input: &T) -> Result<Vec<u8>, Self::Error> {
            serde_json::to_vec(input)
        }

        fn decode(compact: &Vec<u8>) -> Result<T, Self::Error> {
            serde_json::from_slice(compact)
        }
    }

    impl<T: Serialize + DeserializeOwned> Codec<T> for JsonCodec<String> {
        type Compact = String;
        type Error = serde_json::Error;
        fn encode(input: &T) -> Result<String, Self::Error> {
            serde_json::to_string(input)
        }
        fn decode(compact: &String) -> Result<T, Self::Error> {
            serde_json::from_str(&compact)
        }
    }

    impl<T: Serialize + DeserializeOwned> Codec<T> for JsonCodec<Value> {
        type Compact = Value;
        type Error = serde_json::Error;
        fn encode(input: &T) -> Result<Value, Self::Error> {
            serde_json::to_value(input)
        }

        fn decode(compact: &Value) -> Result<T, Self::Error> {
            T::deserialize(compact)
        }
    }
}
