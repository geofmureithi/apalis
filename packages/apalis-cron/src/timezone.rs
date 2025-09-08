use chrono::{NaiveDateTime, TimeZone};

/// Helper trait to synthesize a timezone from offset
pub trait TimeZoneExt: TimeZone {
    /// Get the UTC offset for a naive datetime
    fn utc_offset_from_naive(naive: &NaiveDateTime) -> Self::Offset;
}

impl TimeZoneExt for chrono::Utc {
    fn utc_offset_from_naive(_: &NaiveDateTime) -> Self::Offset {
        chrono::Utc
    }
}

impl TimeZoneExt for chrono::Local {
    fn utc_offset_from_naive(naive: &NaiveDateTime) -> Self::Offset {
        chrono::Local.offset_from_utc_datetime(naive)
    }
}
