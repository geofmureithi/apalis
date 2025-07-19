#[cfg(feature = "tracing")]
#[allow(unused_macros)]
macro_rules! trace {
    ($($tt:tt)*) => {
        tracing::trace!($($tt)*)
    }
}

#[cfg(feature = "tracing")]
#[allow(unused_macros)]
macro_rules! info {
    ($($tt:tt)*) => {
        tracing::info!($($tt)*)
    }
}

#[cfg(feature = "tracing")]
#[allow(unused_macros)]
macro_rules! error {
    ($($tt:tt)*) => {
        tracing::error!($($tt)*)
    };
}

#[cfg(not(feature = "tracing"))]
#[allow(unused_macros)]
macro_rules! trace {
    ($($tt:tt)*) => {};
}

#[cfg(not(feature = "tracing"))]
#[allow(unused_macros)]
macro_rules! info {
    ($($tt:tt)*) => {};
}

#[cfg(not(feature = "tracing"))]
#[allow(unused_macros)]
macro_rules! error {
    ($($tt:tt)*) => {};
}
