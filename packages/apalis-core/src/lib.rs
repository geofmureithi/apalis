#![crate_name = "apalis_core"]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub,
    bad_style,
    dead_code,
    improper_ctypes,
    non_shorthand_field_patterns,
    no_mangle_generic_items,
    overflowing_literals,
    path_statements,
    patterns_in_fns_without_body,
    unconditional_recursion,
    unused,
    unused_allocation,
    unused_comparisons,
    unused_parens,
    while_true
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! # apalis-core
//! Utilities for building job and message processing tools.
/// Represents a task source eg Postgres or Redis
pub mod backend;
/// Includes all possible error types.
pub mod error;
/// Represents monitoring of running workers
pub mod monitor;
/// Represents the request to be processed.
pub mod request;
/// Represents a service that is created from a function.
pub mod service_fn;
/// Represents the utils for building workers.
pub mod worker;

pub mod workflow;
