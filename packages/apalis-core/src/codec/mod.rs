use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use crate::error::BoxDynError;

/// A codec allows backends to encode and decode data
pub trait Codec {
    /// The mode of storage by the codec
    type Compact;
    /// Error encountered by the codec
    type Error: Into<BoxDynError>;
    /// The encoding method
    fn encode<I>(input: I) -> Result<Self::Compact, Self::Error>
    where
        I: Serialize;
    /// The decoding method
    fn decode<O>(input: Self::Compact) -> Result<O, Self::Error>
    where
        O: for<'de> Deserialize<'de>;
}

/// A noop codec to use as a placeholder when backend does not support encoding and decoding
/// Panics if any of its methods are called
#[derive(Debug, Clone)]
pub struct NoopCodec<Compact> {
    compact: PhantomData<Compact>,
}

impl<Compact> Codec for NoopCodec<Compact> {
    type Compact = Compact;
    type Error = BoxDynError;

    fn decode<O>(_: Self::Compact) -> Result<O, Self::Error>
    where
        O: for<'de> Deserialize<'de>,
    {
        unreachable!("NoopCodec doesn't have decoding functionality")
    }
    fn encode<I>(_: I) -> Result<Self::Compact, Self::Error>
    where
        I: Serialize,
    {
        unreachable!("NoopCodec doesn't have decoding functionality")
    }
}

/// Encoding for tasks using json
#[cfg(feature = "json")]
pub mod json;
