use chrono::{DateTime, TimeZone, Utc};

/// Represents a single tick in the cron schedule
#[derive(Debug, Clone)]
pub struct Tick<Tz: TimeZone = Utc> {
    /// The timestamp of the tick
    timestamp: DateTime<Tz>,
}

impl<Tz: TimeZone> Default for Tick<Tz>
where
    DateTime<Tz>: Default,
{
    fn default() -> Self {
        Self {
            timestamp: Default::default(),
        }
    }
}

impl<Tz: TimeZone> Tick<Tz> {
    /// Create a new context provided a timestamp
    pub fn new(timestamp: DateTime<Tz>) -> Self {
        Self { timestamp }
    }

    /// Get the inner timestamp
    pub fn get_timestamp(&self) -> &DateTime<Tz> {
        &self.timestamp
    }
}

#[cfg(feature = "serde")]
mod serde_impl {
    use super::*;
    use crate::{FORMAT, timezone::TimeZoneExt};
    use chrono::NaiveDateTime;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    impl<Tz> Serialize for Tick<Tz>
    where
        Tz: TimeZone,
        Tz::Offset: std::fmt::Display,
    {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let s = self.timestamp.format(FORMAT).to_string();
            serializer.serialize_str(&s)
        }
    }

    impl<'de, Tz> Deserialize<'de> for Tick<Tz>
    where
        Tz: TimeZone + TimeZoneExt,
    {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            let s = String::deserialize(deserializer)?;
            let naive =
                NaiveDateTime::parse_from_str(&s, FORMAT).map_err(serde::de::Error::custom)?;
            let datetime =
                Tz::from_utc_datetime(&Tz::from_offset(&Tz::utc_offset_from_naive(&naive)), &naive);
            Ok(Tick {
                timestamp: datetime,
            })
        }
    }
}
