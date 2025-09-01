//! # Shared backend support for `apalis`
//! 
//! This module provides traits and utilities for sharing backend instances across multiple workers or components within the `apalis` task processing framework.
//! It includes the `MakeShared` trait, which defines how to create shared backend instances, potentially with configuration options.
//! This allows for flexible and reusable backend implementations that can be easily integrated into different parts of an application.
//! 
//! ## Features:
//! - `MakeShared` trait: Defines methods for creating shared backend instances, with or without configuration.
//! - Support for various backend types, enabling code reuse and consistency across workers.
//! - Performance optimizations by allowing backends to reuse connections and resources.

/// Trait for creating shared backend instances
pub trait MakeShared<Args> {
    /// The backend type to be shared
    type Backend;
    /// The Config for the backend
    type Config;
    /// The error returned if the backend cant be shared
    type MakeError;

    /// Returns the backend to be shared
    fn make_shared(&mut self) -> Result<Self::Backend, Self::MakeError>
    where
        Self::Config: Default,
    {
        self.make_shared_with_config(Default::default())
    }

    /// Returns the backend with config
    fn make_shared_with_config(
        &mut self,
        config: Self::Config,
    ) -> Result<Self::Backend, Self::MakeError>;
}
