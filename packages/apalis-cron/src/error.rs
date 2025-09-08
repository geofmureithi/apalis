use core::fmt;
use std::fmt::Display;

use chrono::{DateTime, OutOfRangeError, TimeZone};

use crate::FORMAT;

/// Represents an error emitted by `CronStream` polling
pub enum CronStreamError<Tz: TimeZone> {
    /// The cron stream might not always be polled consistently, such as when the worker is blocked.
    /// If polling is delayed, some ticks may be skipped. When this occurs, an out-of-range error is triggered
    /// because the missed tick is now in the past.
    OutOfRangeError {
        /// The inner error
        inner: OutOfRangeError,
        /// The missed tick
        tick: DateTime<Tz>,
    },
}

impl<Tz: TimeZone> fmt::Display for CronStreamError<Tz>
where
    Tz::Offset: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CronStreamError::OutOfRangeError { inner, tick } => {
                write!(
                    f,
                    "Cron tick {} is out of range: {}",
                    tick.format(FORMAT),
                    inner
                )
            }
        }
    }
}

impl<Tz: TimeZone> std::error::Error for CronStreamError<Tz>
where
    Tz::Offset: Display,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CronStreamError::OutOfRangeError { inner, .. } => Some(inner),
        }
    }
}

impl<Tz: TimeZone> fmt::Debug for CronStreamError<Tz> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CronStreamError::OutOfRangeError { inner, tick } => f
                .debug_struct("OutOfRangeError")
                .field("tick", tick)
                .field("inner", inner)
                .finish(),
        }
    }
}
