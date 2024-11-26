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

/// Encoding for tasks using json
#[cfg(feature = "json")]
pub mod json;
