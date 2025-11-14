use apalis_core::task::task_id::RandomId;

/// Trait for generating unique IDs
pub trait GenerateId {
    /// Generate a new unique ID
    fn generate() -> Self;
}

#[cfg(feature = "uuid")]
impl GenerateId for uuid::Uuid {
    fn generate() -> Self {
        uuid::Uuid::new_v4()
    }
}

#[cfg(feature = "ulid")]
impl GenerateId for ulid::Ulid {
    fn generate() -> Self {
        Self::new()
    }
}

impl GenerateId for RandomId {
    fn generate() -> Self {
        Self::default()
    }
}
